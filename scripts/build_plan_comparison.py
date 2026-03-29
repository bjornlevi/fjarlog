#!/usr/bin/env python3
"""
Build budget plan comparison dataset with all forecast values from all plan documents.
Creates a unified dataset for the fjármálaáætlun (budget plan) page.
"""

import logging
from pathlib import Path
import pandas as pd

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

OUTPUT_FILE = CURATED_DIR / "plan_comparison.parquet"


def build_plan_comparison():
    """Build comparison of all plan forecasts across documents and years."""

    plans_dir = PROCESSED_DIR / "budget_plans"
    plans_data = []

    # Load all plan documents
    for parquet_file in sorted(plans_dir.glob("plan_*_malefnasvid.parquet")):
        df = pd.read_parquet(parquet_file)

        # Extract plan year range from filename (e.g., "plan_2025_2029_malefnasvid.parquet")
        parts = parquet_file.stem.split('_')
        if len(parts) >= 3:
            plan_start_year = parts[1]
            plan_end_year = parts[2]
            plan_range = f"{plan_start_year}-{plan_end_year}"
        else:
            plan_range = "unknown"

        df['plan_range'] = plan_range
        plans_data.append(df)
        logger.info(f"Loaded {parquet_file.name}: {len(df)} rows ({plan_range})")

    if not plans_data:
        logger.warning("No plan data found")
        return None

    # Concatenate all plan data (keeping duplicates - we want all plans)
    df_plans = pd.concat(plans_data, ignore_index=True)
    logger.info(f"Total plan data: {len(df_plans)} rows")

    # Sort by year and plan range for consistent display
    df_plans = df_plans.sort_values(['year', 'malefnasvid_nr', 'plan_range']).reset_index(drop=True)

    # Log summary
    logger.info(f"\nPlan comparison dataset summary:")
    logger.info(f"  Total rows: {len(df_plans)}")
    logger.info(f"  Years: {sorted(df_plans['year'].unique())}")
    logger.info(f"  Policy areas: {df_plans['malefnasvid_nr'].nunique()}")

    # Sample output
    logger.info(f"\nSample rows (2024):")
    sample = df_plans[df_plans['year'] == 2024].head(5)
    for _, row in sample.iterrows():
        logger.info(f"  {row['malefnasvid_nr']} {row['malefnasvid'][:30]:30} - Plan {row['plan_range']}: {row['amount']:>8,.0f} ma.kr.")

    return df_plans


if __name__ == "__main__":
    logger.info("Building budget plan comparison dataset...")
    df = build_plan_comparison()

    if df is not None:
        # Save to parquet
        df.to_parquet(OUTPUT_FILE, compression="snappy", index=False)
        logger.info(f"\nSaved plan comparison data to: {OUTPUT_FILE}")
