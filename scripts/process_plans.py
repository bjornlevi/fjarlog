#!/usr/bin/env python3
"""
Extract budget line items from XLSX budget plan files into parquet format.
Part of the silver layer processing.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
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
LANDING_DIR = PROJECT_DIR / "data" / "landing" / "budget_plans"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed" / "budget_plans"

# Create output directory
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def find_xlsx_files() -> List[Path]:
    """Find all XLSX files in the budget plans landing directory."""
    return sorted(LANDING_DIR.glob("*/plan_*.xlsx"))


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
        logger.info(f"Processing: {file_path.name}")

        # Read Excel file
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        logger.info(f"  Sheets: {xls.sheet_names}")

        # Try to find the main data sheet (usually first or named "data", "budget", etc.)
        data_sheet = None
        for sheet_name in xls.sheet_names:
            if sheet_name.lower() in ["data", "budget", "gögn", "fjárlög", "fjarlög"]:
                data_sheet = sheet_name
                break

        if not data_sheet:
            data_sheet = xls.sheet_names[0]

        logger.info(f"  Using sheet: {data_sheet}")

        # Read the sheet
        df = pd.read_excel(file_path, sheet_name=data_sheet, engine='openpyxl')

        logger.info(f"  Shape: {df.shape}")
        logger.info(f"  Columns: {df.columns.tolist()}")

        # Extract year/period from filename or sheet
        # Format: plan_{start_year}_{end_year}.xlsx
        match = re.search(r"plan_(\d{4})_(\d{4})", file_path.name)
        if match:
            start_year, end_year = int(match.group(1)), int(match.group(2))
            doc_id = f"plan_{start_year}_{end_year}"
        else:
            logger.warning(f"Could not parse year from filename: {file_path.name}")
            return None

        # For now, create a simple extraction that captures the basic structure
        # This may need refinement based on actual data structure
        result_rows = []

        # Try to standardize the data
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isnull().all():
                continue

            # Create a record for each year in the plan period
            for year in range(start_year, end_year + 1):
                # Try to find amount columns (usually numeric)
                amount = None

                # Look for numeric columns that might represent amounts
                for col in df.columns:
                    val = row[col]
                    if pd.notna(val) and isinstance(val, (int, float)):
                        amount = val
                        break

                if amount is not None:
                    result_rows.append({
                        "year": year,
                        "source_type": "plan",
                        "document_id": doc_id,
                        "institution": str(row[df.columns[0]]) if pd.notna(row[df.columns[0]]) else "Unknown",
                        "budget_line": str(row[df.columns[1]]) if len(df.columns) > 1 and pd.notna(row[df.columns[1]]) else "Total",
                        "amount": amount,
                    })

        if result_rows:
            logger.info(f"  Extracted {len(result_rows)} records")
            return pd.DataFrame(result_rows)
        else:
            logger.warning(f"  No records extracted from {file_path.name}")
            return None

    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        return None


def process_all_plans() -> None:
    """Process all XLSX budget plan files."""
    xlsx_files = find_xlsx_files()

    if not xlsx_files:
        logger.warning(f"No XLSX files found in {LANDING_DIR}")
        return

    logger.info(f"Found {len(xlsx_files)} plan files to process")

    processed_count = 0
    for file_path in xlsx_files:
        df = extract_from_xlsx(file_path)

        if df is not None and not df.empty:
            # Determine output filename
            match = re.search(r"plan_(\d{4})_(\d{4})", file_path.name)
            if match:
                start_year, end_year = match.group(1), match.group(2)
                output_file = PROCESSED_DIR / f"plan_{start_year}_{end_year}.parquet"

                # Write to parquet
                df.to_parquet(output_file, compression="snappy")
                logger.info(f"  Saved: {output_file.name}")
                processed_count += 1

    logger.info(f"\nProcessed {processed_count}/{len(xlsx_files)} plan files successfully")


if __name__ == "__main__":
    process_all_plans()
