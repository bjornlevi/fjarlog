#!/usr/bin/env python3
"""
Build malefnasvið comparison dataset by joining bills, accounts, and plans on (year, malefnasvid_nr).
Creates a unified dataset for the comparison dashboard.
"""

import logging
from pathlib import Path
import pandas as pd
import numpy as np

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
CURATED_DIR = PROJECT_DIR / "data" / "curated"

# Create output directory
CURATED_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = CURATED_DIR / "malefnasvid_comparison.parquet"


def load_all_sources():
    """Load and prepare data from all three sources."""

    # Load bills - using malefnasvid version when available (2020-2026)
    bills_data = []
    bills_dir = PROCESSED_DIR / "budget_bills"

    # Try malefnasvid files first (2020-2026), fall back to regular files
    for year in range(2020, 2027):
        # Try malefnasvid file first
        malefnasvid_file = bills_dir / f"bill_{year}_malefnasvid.parquet"
        regular_file = bills_dir / f"bill_{year}.parquet"

        parquet_file = None
        if malefnasvid_file.exists():
            parquet_file = malefnasvid_file
        elif regular_file.exists():
            parquet_file = regular_file

        if parquet_file:
            df = pd.read_parquet(parquet_file)
            df['source'] = 'bill'
            bills_data.append(df)
            logger.info(f"Loaded bills for {year}: {len(df)} rows")

    if bills_data:
        df_bills = pd.concat(bills_data, ignore_index=True)
        logger.info(f"Total bills data: {len(df_bills)} rows across {df_bills['year'].nunique()} years")
    else:
        df_bills = pd.DataFrame(columns=['year', 'malefnasvid_nr', 'malefnasvid', 'amount', 'source'])
        logger.warning("No bills data found")

    # Load accounts (2015-2025)
    accounts_data = []
    accounts_dir = PROCESSED_DIR / "budget_accounts"
    for year in range(2015, 2026):
        parquet_file = accounts_dir / f"accounts_{year}.parquet"
        if parquet_file.exists():
            df = pd.read_parquet(parquet_file)
            df['source'] = 'accounts'
            accounts_data.append(df)
            logger.info(f"Loaded accounts for {year}: {len(df)} rows")

    if accounts_data:
        df_accounts = pd.concat(accounts_data, ignore_index=True)
        logger.info(f"Total accounts data: {len(df_accounts)} rows across {df_accounts['year'].nunique()} years")
    else:
        df_accounts = pd.DataFrame(columns=['year', 'malefnasvid_nr', 'malefnasvid', 'amount', 'source'])
        logger.warning("No accounts data found")

    # Load plans (2024-2030 from various plan documents)
    plans_data = []
    plans_dir = PROCESSED_DIR / "budget_plans"
    for parquet_file in sorted(plans_dir.glob("plan_*_malefnasvid.parquet")):
        df = pd.read_parquet(parquet_file)
        df['source'] = 'plan'
        plans_data.append(df)
        logger.info(f"Loaded {parquet_file.name}: {len(df)} rows")

    if plans_data:
        df_plans = pd.concat(plans_data, ignore_index=True)
        # Keep only the first occurrence of each (year, malefnasvid_nr) pair
        # (prefer earlier plan documents)
        df_plans = df_plans.drop_duplicates(subset=['year', 'malefnasvid_nr'], keep='first')
        logger.info(f"Total plans data: {len(df_plans)} rows across {df_plans['year'].nunique()} years (deduplicated)")
    else:
        df_plans = pd.DataFrame(columns=['year', 'malefnasvid_nr', 'malefnasvid', 'amount', 'source'])
        logger.warning("No plans data found")

    return df_bills, df_accounts, df_plans


def build_comparison():
    """Build the unified comparison dataset."""
    df_bills, df_accounts, df_plans = load_all_sources()

    # Get the set of all years
    all_years = set()
    if not df_bills.empty:
        all_years.update(df_bills['year'].unique())
    if not df_accounts.empty:
        all_years.update(df_accounts['year'].unique())
    if not df_plans.empty:
        all_years.update(df_plans['year'].unique())

    # Standard 35 policy area codes (01-35)
    standard_areas = [f"{i:02d}" for i in range(1, 36)]

    # Create all combinations of year and standard policy areas
    all_pairs = set()
    for year in all_years:
        for area in standard_areas:
            all_pairs.add((year, area))

    logger.info(f"Found {len(all_years)} unique years")
    logger.info(f"Creating entries for {len(all_pairs)} (year, malefnasvid_nr) pairs (35 areas × {len(all_years)} years)")

    # Build a mapping of malefnasvid_nr to name from all sources
    malefnasvid_names = {}
    for _, row in df_bills.iterrows():
        if row['malefnasvid_nr'] not in malefnasvid_names:
            malefnasvid_names[row['malefnasvid_nr']] = row.get('malefnasvid')
    for _, row in df_accounts.iterrows():
        if row['malefnasvid_nr'] not in malefnasvid_names:
            malefnasvid_names[row['malefnasvid_nr']] = row.get('malefnasvid')
    for _, row in df_plans.iterrows():
        if row['malefnasvid_nr'] not in malefnasvid_names:
            malefnasvid_names[row['malefnasvid_nr']] = row.get('malefnasvid')

    # Build result rows
    result_rows = []

    for year, malefnasvid_nr in sorted(all_pairs):
        # Get data from each source
        bill_row = None
        if not df_bills.empty:
            matching = df_bills[(df_bills['year'] == year) & (df_bills['malefnasvid_nr'] == malefnasvid_nr)]
            if not matching.empty:
                bill_row = matching.iloc[0]

        account_row = None
        if not df_accounts.empty:
            matching = df_accounts[(df_accounts['year'] == year) & (df_accounts['malefnasvid_nr'] == malefnasvid_nr)]
            if not matching.empty:
                account_row = matching.iloc[0]

        plan_row = None
        if not df_plans.empty:
            matching = df_plans[(df_plans['year'] == year) & (df_plans['malefnasvid_nr'] == malefnasvid_nr)]
            if not matching.empty:
                plan_row = matching.iloc[0]

        # Get malefnasvid name from mapping
        malefnasvid_name = malefnasvid_names.get(malefnasvid_nr)

        # Extract amounts
        amount_billed = bill_row['amount'] if bill_row is not None else None
        amount_actual = account_row['amount'] if account_row is not None else None
        amount_planned = plan_row['amount'] if plan_row is not None else None

        result_rows.append({
            'year': int(year),
            'malefnasvid_nr': malefnasvid_nr,
            'malefnasvid': malefnasvid_name,
            'amount_planned': amount_planned,
            'amount_billed': amount_billed,
            'amount_actual': amount_actual,
        })

    df_result = pd.DataFrame(result_rows)

    # Log summary statistics
    logger.info(f"\nComparison dataset summary:")
    logger.info(f"  Total rows: {len(df_result)}")
    logger.info(f"  Years: {sorted(df_result['year'].unique())}")
    logger.info(f"  Planned values: {df_result['amount_planned'].notna().sum()}")
    logger.info(f"  Billed values: {df_result['amount_billed'].notna().sum()}")
    logger.info(f"  Actual values: {df_result['amount_actual'].notna().sum()}")

    # Sample output
    logger.info(f"\nSample rows (2024):")
    sample = df_result[df_result['year'] == 2024].head(5)
    for _, row in sample.iterrows():
        logger.info(f"  {row['malefnasvid_nr']} {row['malefnasvid'][:30]:30} - Plan: {row['amount_planned']}, Bill: {row['amount_billed']}, Actual: {row['amount_actual']}")

    return df_result


if __name__ == "__main__":
    logger.info("Building malefnasvið comparison dataset...")
    df = build_comparison()

    # Save to parquet
    df.to_parquet(OUTPUT_FILE, compression="snappy", index=False)
    logger.info(f"\nSaved comparison data to: {OUTPUT_FILE}")
