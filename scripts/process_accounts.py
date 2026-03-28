#!/usr/bin/env python3
"""
Extract budget account data from Ríkisreikningur (government accounts) CSV files into parquet format.
Part of the silver layer processing.

Data columns: institution, budget line, amount (actual spending)
"""

import logging
from pathlib import Path
from typing import Optional, List
import pandas as pd
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
LANDING_DIR = PROJECT_DIR / "data" / "landing" / "budget_accounts"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed" / "budget_accounts"

# Create output directory
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def find_csv_files() -> List[Path]:
    """Find all Ríkisreikningur CSV files in the budget accounts landing directory."""
    return sorted(LANDING_DIR.glob("*.csv"))


def extract_from_csv(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract budget account data from a Ríkisreikningur CSV file.

    CSV columns: TimabilAr, RaduneytiHeiti, StofnunHeiti, MalaflokkurHeiti, MalefnasvidNumer, MalefnasvidHeiti, Samtals (amount)

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame with columns: year, source_type, document_id, malefnasvid_nr, malefnasvid, institution, budget_line, amount
        Or None if extraction fails
    """
    try:
        logger.info(f"Processing: {file_path.name}")

        # Extract year from filename (e.g., Rikisreikningur_gogn_2025_06.csv or Rikisreikningur_gogn_2024.csv)
        match = re.search(r"(\d{4})", file_path.name)
        if not match:
            logger.warning(f"Could not parse year from filename: {file_path.name}")
            return None

        year = int(match.group(1))
        doc_id = f"accounts_{year}"

        # Read CSV file
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        logger.info(f"  Shape: {df.shape}")
        logger.info(f"  Columns: {df.columns.tolist()}")

        result_rows = []

        # Expected columns for Ríkisreikningur data
        required_cols = ['TimabilAr', 'RaduneytiHeiti', 'MalaflokkurHeiti', 'MalefnasvidNumer', 'MalefnasvidHeiti', 'Samtals']
        has_malefnasvid = all(col in df.columns for col in ['MalefnasvidNumer', 'MalefnasvidHeiti'])
        has_required = all(col in df.columns for col in ['TimabilAr', 'RaduneytiHeiti', 'MalaflokkurHeiti', 'Samtals'])

        if not has_required:
            logger.warning(f"  Missing required columns. Found: {df.columns.tolist()}")
            return None

        # Extract rows
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isnull().all():
                continue

            try:
                # Get year from data (should match file year)
                row_year = int(row['TimabilAr']) if pd.notna(row['TimabilAr']) else None
                if row_year != year:
                    continue

                # Get málefnasvið (policy area) if available
                malefnasvid_nr = None
                malefnasvid = None
                if has_malefnasvid:
                    malefnasvid_nr = str(int(row['MalefnasvidNumer'])).zfill(2) if pd.notna(row['MalefnasvidNumer']) else None
                    malefnasvid_name = str(row['MalefnasvidHeiti']).strip() if pd.notna(row['MalefnasvidHeiti']) else None
                    if malefnasvid_nr and malefnasvid_name:
                        malefnasvid = f"{malefnasvid_nr} {malefnasvid_name}"

                # Get institution (ministry)
                institution = str(row['RaduneytiHeiti']).strip() if pd.notna(row['RaduneytiHeiti']) else "Unknown"

                # Get budget line (category)
                budget_line = str(row['MalaflokkurHeiti']).strip() if pd.notna(row['MalaflokkurHeiti']) else "Unknown"

                # Get amount (actual spending)
                # Ríkisreikningur amounts are in ISK; convert to millions (ma.kr.) to match bills/plans
                amount = None
                if pd.notna(row['Samtals']):
                    try:
                        amount = float(row['Samtals']) / 1_000_000
                    except (ValueError, TypeError):
                        amount_str = str(row['Samtals']).strip()
                        # Handle Icelandic number format (comma as decimal, period as thousands)
                        amount_str = amount_str.replace(".", "").replace(",", ".")
                        amount = float(amount_str) / 1_000_000

                # Only include non-zero amounts and records with malefnasvid
                if amount is not None and amount != 0 and malefnasvid_nr:
                    result_rows.append({
                        "year": year,
                        "source_type": "accounts",
                        "document_id": doc_id,
                        "malefnasvid_nr": malefnasvid_nr,
                        "malefnasvid": malefnasvid,
                        "institution": institution,
                        "budget_line": budget_line,
                        "amount": amount,
                    })

            except (ValueError, IndexError, KeyError) as e:
                logger.debug(f"  Skipping row {idx}: {e}")
                continue

        if result_rows:
            # Group by malefnasvid_nr and sum amounts
            df_result = pd.DataFrame(result_rows)
            df_grouped = df_result.groupby(["malefnasvid_nr", "malefnasvid"], as_index=False).agg({
                "year": "first",
                "source_type": "first",
                "document_id": "first",
                "amount": "sum"
            })
            logger.info(f"  Extracted {len(df_result)} records, aggregated to {len(df_grouped)} málefnasvið")
            return df_grouped
        else:
            logger.warning(f"  No records extracted from {file_path.name}")
            return None

    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        return None


def process_all_accounts() -> None:
    """Process all Ríkisreikningur budget account data files."""
    csv_files = find_csv_files()

    if not csv_files:
        logger.warning(f"No CSV files found in {LANDING_DIR}")
        return

    logger.info(f"Found {len(csv_files)} account data files to process")

    processed_count = 0
    processed_years = set()

    for file_path in csv_files:
        df = extract_from_csv(file_path)

        if df is not None and not df.empty:
            year = df["year"].iloc[0]
            if year not in processed_years:
                output_file = PROCESSED_DIR / f"accounts_{year}.parquet"
                df.to_parquet(output_file, compression="snappy")
                logger.info(f"  Saved: {output_file.name}")
                processed_count += 1
                processed_years.add(year)

    logger.info(f"\nProcessed {processed_count}/{len(csv_files)} account files successfully")


if __name__ == "__main__":
    process_all_accounts()
