#!/usr/bin/env python3
"""
Build comparison parquet by joining processed plan, bill, and account data.
Creates the curated/gold layer.
"""

import logging
from pathlib import Path
from typing import List, Tuple
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
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
CURATED_DIR = PROJECT_DIR / "data" / "curated"

# Create output directory
CURATED_DIR.mkdir(parents=True, exist_ok=True)


def load_processed_files(source_type: str) -> pd.DataFrame:
    """
    Load all processed parquet files for a given source type.

    Args:
        source_type: 'plans', 'bills', or 'accounts'

    Returns:
        Combined DataFrame
    """
    parquet_dir = PROCESSED_DIR / f"budget_{source_type}"

    if not parquet_dir.exists():
        logger.warning(f"Directory not found: {parquet_dir}")
        return pd.DataFrame()

    parquet_files = sorted(parquet_dir.glob("*.parquet"))

    if not parquet_files:
        logger.warning(f"No parquet files found in {parquet_dir}")
        return pd.DataFrame()

    logger.info(f"Loading {len(parquet_files)} {source_type} files...")

    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            logger.info(f"  Loaded: {file.name} ({len(df)} rows)")
            dfs.append(df)
        except Exception as e:
            logger.warning(f"  Failed to load {file.name}: {e}")

    if dfs:
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"Combined {len(dfs)} files: {len(combined)} total rows")
        return combined
    else:
        return pd.DataFrame()


def get_plan_year_range(doc_id: str) -> Tuple[int, int]:
    """
    Extract year range from plan document ID.

    Args:
        doc_id: e.g., 'plan_2025_2029'

    Returns:
        Tuple of (start_year, end_year)
    """
    match = re.search(r"plan_(\d{4})_(\d{4})", doc_id)
    if match:
        return int(match.group(1)), int(match.group(2))
    return None, None


def build_comparison() -> None:
    """Build the comparison parquet from processed data."""
    logger.info("Loading processed data...")

    plans_df = load_processed_files("plans")
    bills_df = load_processed_files("bills")
    accounts_df = load_processed_files("accounts")

    # Normalize column names (skip empty dataframes)
    for df in [plans_df, bills_df, accounts_df]:
        if not df.empty:
            df.columns = [col.lower() if isinstance(col, str) else str(col).lower() for col in df.columns]

    if plans_df.empty and bills_df.empty and accounts_df.empty:
        logger.error("No processed data found!")
        return

    logger.info("\nBuilding comparison table...")

    # Create a comparison table
    # Key: (year, budget_line, institution)
    comparison_rows = []

    # Collect all unique (year, budget_line) combinations
    unique_entries = set()

    for _, row in bills_df.iterrows():
        unique_entries.add((row["year"], row["budget_line"], row["institution"]))

    for _, row in accounts_df.iterrows():
        unique_entries.add((row["year"], row["budget_line"], row["institution"]))

    for _, row in plans_df.iterrows():
        unique_entries.add((row["year"], row["budget_line"], row["institution"]))

    logger.info(f"Found {len(unique_entries)} unique (year, budget_line, institution) combinations")

    # Build comparison rows
    for year, budget_line, institution in sorted(unique_entries):
        # Find matching plan (earliest plan that covers this year)
        planned_amount = None
        plan_doc_id = None

        for _, row in plans_df.iterrows():
            if row["budget_line"] == budget_line and row["institution"] == institution:
                start_year, end_year = get_plan_year_range(row["document_id"])
                if start_year and start_year <= year <= end_year:
                    if plan_doc_id is None or start_year > int(plan_doc_id.split("_")[1]):
                        planned_amount = row["amount"]
                        plan_doc_id = row["document_id"]

        # Find matching bill
        billed_amount = None
        bill_doc_id = None

        for _, row in bills_df.iterrows():
            if (
                row["year"] == year
                and row["budget_line"] == budget_line
                and row["institution"] == institution
            ):
                billed_amount = row["amount"]
                bill_doc_id = row["document_id"]
                break

        # Find matching account
        actual_amount = None
        account_doc_id = None

        for _, row in accounts_df.iterrows():
            if (
                row["year"] == year
                and row["budget_line"] == budget_line
                and row["institution"] == institution
            ):
                actual_amount = row["amount"]
                account_doc_id = row["document_id"]
                break

        comparison_rows.append({
            "year": year,
            "institution": institution,
            "budget_line": budget_line,
            "amount_planned": planned_amount,
            "amount_billed": billed_amount,
            "amount_actual": actual_amount,
            "plan_document": plan_doc_id,
            "bill_document": bill_doc_id,
            "account_document": account_doc_id,
        })

    if comparison_rows:
        comparison_df = pd.DataFrame(comparison_rows)

        # Sort by year and institution
        comparison_df = comparison_df.sort_values(["year", "institution", "budget_line"])

        # Save to parquet
        output_file = CURATED_DIR / "comparison.parquet"
        comparison_df.to_parquet(output_file, compression="snappy")

        logger.info(f"\nSaved comparison to: {output_file}")
        logger.info(f"Total rows: {len(comparison_df)}")
        logger.info(f"\nSample rows:")
        logger.info(comparison_df.head(10).to_string())
    else:
        logger.warning("No comparison data generated")


if __name__ == "__main__":
    build_comparison()
