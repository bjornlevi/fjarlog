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
    """Build the comparison parquet from processed data using fully vectorized operations."""
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

    # Expand plans to all covered years (vectorized)
    if not plans_df.empty:
        plans_expanded = []
        for _, row in plans_df.iterrows():
            start_year, end_year = get_plan_year_range(row["document_id"])
            if start_year and end_year:
                years = list(range(start_year, end_year + 1))
                plan_copy = row.to_dict()
                plan_copy["year"] = years
                plans_expanded.append(plan_copy)

        if plans_expanded:
            # Convert to dataframe and explode year column
            plans_expanded_df = pd.DataFrame(plans_expanded)
            plans_df_expanded = plans_expanded_df.explode("year", ignore_index=True)
        else:
            plans_df_expanded = pd.DataFrame()
    else:
        plans_df_expanded = pd.DataFrame()

    # Collect all unique (year, budget_line, institution) from bills and accounts
    all_entries = []
    if not bills_df.empty:
        all_entries.append(bills_df[["year", "budget_line", "institution"]].drop_duplicates())
    if not accounts_df.empty:
        all_entries.append(accounts_df[["year", "budget_line", "institution"]].drop_duplicates())
    if not plans_df_expanded.empty:
        all_entries.append(plans_df_expanded[["year", "budget_line", "institution"]].drop_duplicates())

    if all_entries:
        unique_entries = pd.concat(all_entries, ignore_index=True).drop_duplicates().reset_index(drop=True)
    else:
        unique_entries = pd.DataFrame()

    logger.info(f"Found {len(unique_entries)} unique (year, budget_line, institution) combinations")

    if unique_entries.empty:
        logger.warning("No entries found!")
        return

    # Merge bills data
    if not bills_df.empty:
        bills_merged = bills_df[["year", "budget_line", "institution", "amount", "document_id"]].copy()
        bills_merged.columns = ["year", "budget_line", "institution", "amount_billed", "bill_document"]
        unique_entries = unique_entries.merge(
            bills_merged,
            on=["year", "budget_line", "institution"],
            how="left"
        )
    else:
        unique_entries["amount_billed"] = None
        unique_entries["bill_document"] = None

    # Merge accounts data
    if not accounts_df.empty:
        accounts_merged = accounts_df[["year", "budget_line", "institution", "amount", "document_id"]].copy()
        accounts_merged.columns = ["year", "budget_line", "institution", "amount_actual", "account_document"]
        unique_entries = unique_entries.merge(
            accounts_merged,
            on=["year", "budget_line", "institution"],
            how="left"
        )
    else:
        unique_entries["amount_actual"] = None
        unique_entries["account_document"] = None

    # Merge plans data - for each (year, budget_line, institution), get the best matching plan
    if not plans_df_expanded.empty:
        # Group by (year, budget_line, institution) and take the plan with latest start_year
        plans_best = plans_df_expanded.copy()

        # Add start_year for sorting
        plans_best["start_year"] = plans_best["document_id"].apply(
            lambda x: int(x.split("_")[1]) if "_" in x else 0
        )

        # Sort by start_year descending and take first (latest plan)
        plans_best = plans_best.sort_values("start_year", ascending=False).drop_duplicates(
            subset=["year", "budget_line", "institution"],
            keep="first"
        )

        plans_merged = plans_best[["year", "budget_line", "institution", "amount", "document_id"]].copy()
        plans_merged.columns = ["year", "budget_line", "institution", "amount_planned", "plan_document"]

        unique_entries = unique_entries.merge(
            plans_merged,
            on=["year", "budget_line", "institution"],
            how="left"
        )
    else:
        unique_entries["amount_planned"] = None
        unique_entries["plan_document"] = None

    # Reorder columns
    comparison_df = unique_entries[[
        "year", "institution", "budget_line",
        "amount_planned", "amount_billed", "amount_actual",
        "plan_document", "bill_document", "account_document"
    ]].copy()

    # Sort by year and institution
    comparison_df = comparison_df.sort_values(["year", "institution", "budget_line"]).reset_index(drop=True)

    # Save to parquet
    output_file = CURATED_DIR / "comparison.parquet"
    comparison_df.to_parquet(output_file, compression="snappy")

    logger.info(f"\nSaved comparison to: {output_file}")
    logger.info(f"Total rows: {len(comparison_df)}")
    logger.info(f"\nSample rows:")
    logger.info(comparison_df.head(10).to_string())


if __name__ == "__main__":
    build_comparison()
