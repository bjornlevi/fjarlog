#!/usr/bin/env python3
"""
Extract budget line items from CSV/XLSX budget account data files into parquet format.
Part of the silver layer processing.
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
    """Find all CSV files in the budget accounts landing directory."""
    return sorted(LANDING_DIR.glob("*/*.csv"))


def find_xlsx_files() -> List[Path]:
    """Find all XLSX files in the budget accounts landing directory."""
    return sorted(LANDING_DIR.glob("*/*.xlsx"))


def extract_from_csv(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract budget data from a CSV file.

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame with columns: year, source_type, document_id, institution, budget_line, amount
        Or None if extraction fails
    """
    try:
        logger.info(f"Processing CSV: {file_path.name}")

        # Extract year from parent directory name
        year_dir = file_path.parent.name
        match = re.search(r"(\d{4})", year_dir)
        if not match:
            logger.warning(f"Could not parse year from directory: {year_dir}")
            return None

        year = int(match.group(1))
        doc_id = f"accounts_{year}"

        # Read CSV file
        df = pd.read_csv(file_path)

        logger.info(f"  Shape: {df.shape}")
        logger.info(f"  Columns: {df.columns.tolist()}")

        result_rows = []

        # Extract rows - assuming first column is institution, second is category, rest are amounts
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isnull().all():
                continue

            # Try to extract institution and budget line
            institution = str(row.iloc[0]) if pd.notna(row.iloc[0]) else "Unknown"
            budget_line = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else "Total"

            # Find first numeric value as amount
            amount = None
            for val in row:
                if pd.notna(val):
                    try:
                        amount = float(str(val).replace(",", "").replace(".", ""))
                        break
                    except (ValueError, AttributeError):
                        continue

            if amount is not None:
                result_rows.append({
                    "year": year,
                    "source_type": "accounts",
                    "document_id": doc_id,
                    "institution": institution,
                    "budget_line": budget_line,
                    "amount": amount,
                })

        if result_rows:
            logger.info(f"  Extracted {len(result_rows)} records")
            return pd.DataFrame(result_rows)
        else:
            logger.warning(f"  No records extracted from {file_path.name}")
            return None

    except Exception as e:
        logger.error(f"Error processing CSV {file_path.name}: {e}")
        return None


def extract_from_xlsx(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract budget data from an XLSX file.

    Args:
        file_path: Path to the XLSX file

    Returns:
        DataFrame with columns: year, source_type, document_id, institution, budget_line, amount
        Or None if extraction fails
    """
    try:
        logger.info(f"Processing XLSX: {file_path.name}")

        # Extract year from parent directory name
        year_dir = file_path.parent.name
        match = re.search(r"(\d{4})", year_dir)
        if not match:
            logger.warning(f"Could not parse year from directory: {year_dir}")
            return None

        year = int(match.group(1))
        doc_id = f"accounts_{year}"

        # Read XLSX file
        xls = pd.ExcelFile(file_path)
        logger.info(f"  Sheets: {xls.sheet_names}")

        # Try to find the main data sheet
        data_sheet = xls.sheet_names[0]

        # Read the sheet
        df = pd.read_excel(file_path, sheet_name=data_sheet)

        logger.info(f"  Shape: {df.shape}")

        result_rows = []

        # Extract rows
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isnull().all():
                continue

            # Extract institution and amount
            institution = str(row.iloc[0]) if pd.notna(row.iloc[0]) else "Unknown"
            budget_line = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else "Total"

            # Find first numeric value as amount
            amount = None
            for val in row:
                if pd.notna(val):
                    try:
                        amount = float(str(val).replace(",", "").replace(".", ""))
                        break
                    except (ValueError, AttributeError):
                        continue

            if amount is not None:
                result_rows.append({
                    "year": year,
                    "source_type": "accounts",
                    "document_id": doc_id,
                    "institution": institution,
                    "budget_line": budget_line,
                    "amount": amount,
                })

        if result_rows:
            logger.info(f"  Extracted {len(result_rows)} records")
            return pd.DataFrame(result_rows)
        else:
            logger.warning(f"  No records extracted from {file_path.name}")
            return None

    except Exception as e:
        logger.error(f"Error processing XLSX {file_path.name}: {e}")
        return None


def process_all_accounts() -> None:
    """Process all budget account data files (CSV and XLSX)."""
    csv_files = find_csv_files()
    xlsx_files = find_xlsx_files()

    all_files = csv_files + xlsx_files

    if not all_files:
        logger.warning(f"No account data files found in {LANDING_DIR}")
        return

    logger.info(f"Found {len(all_files)} account data files to process ({len(csv_files)} CSV, {len(xlsx_files)} XLSX)")

    processed_count = 0
    processed_years = set()

    # Process CSV files
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

    # Process XLSX files (if year not already processed from CSV)
    for file_path in xlsx_files:
        df = extract_from_xlsx(file_path)

        if df is not None and not df.empty:
            year = df["year"].iloc[0]
            if year not in processed_years:
                output_file = PROCESSED_DIR / f"accounts_{year}.parquet"
                df.to_parquet(output_file, compression="snappy")
                logger.info(f"  Saved: {output_file.name}")
                processed_count += 1
                processed_years.add(year)

    logger.info(f"\nProcessed {processed_count} account years successfully")


if __name__ == "__main__":
    process_all_accounts()
