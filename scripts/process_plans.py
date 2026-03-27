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
    Extract budget data from an XLSX file (budget plan format).

    Budget plan format: first column = budget line categories,
    subsequent columns = year estimates, with headers like "Áætlun 2025"

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
            # Use first sheet that's not a table of contents
            for sheet_name in xls.sheet_names:
                if "tafla" in sheet_name.lower() or "tafl" in sheet_name.lower():
                    data_sheet = sheet_name
                    break

        if not data_sheet:
            data_sheet = xls.sheet_names[0]

        logger.info(f"  Using sheet: {data_sheet}")

        # Read the sheet - try row 3 (index 2) as header first
        # This works for budget plan files where row 3 contains the year headers
        df = pd.read_excel(file_path, sheet_name=data_sheet, engine='openpyxl', header=2)

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

        result_rows = []

        # First column contains budget line categories
        category_col = df.columns[0]

        # Find columns for the plan period years
        # Headers might be like "Áætlun 2025" or just "2025"
        year_columns = {}
        for col in df.columns[1:]:
            col_str = str(col).strip()
            # Handle multiline headers by replacing newlines
            col_str = col_str.replace('\n', ' ').replace('  ', ' ')

            # Look for year patterns
            year_match = re.search(r'(\d{4})', col_str)
            if year_match:
                year = int(year_match.group(1))
                # Only include years in the plan period
                if start_year <= year <= end_year:
                    year_columns[year] = col
                    logger.debug(f"  Found year {year} in column '{col}' ({col_str})")

        logger.info(f"  Found plan columns for years: {sorted(year_columns.keys())}")

        if not year_columns:
            logger.warning(f"  No plan year columns found in sheet")
            return None

        # Iterate through rows and extract budget lines
        for idx, row in df.iterrows():
            budget_line = row[category_col]

            # Skip empty rows and header rows
            if pd.isna(budget_line):
                continue

            budget_line = str(budget_line).strip()
            if not budget_line or budget_line.startswith("Unnamed") or len(budget_line) < 2:
                continue

            # Extract amounts for each planned year
            for year, year_col in year_columns.items():
                val = row[year_col]

                if pd.notna(val):
                    try:
                        # Parse amount (handle numeric and string formats)
                        if isinstance(val, (int, float)):
                            amount = float(val)
                        else:
                            amount_str = str(val).strip()
                            # Handle Icelandic number format (comma decimal, period thousands)
                            amount_str = amount_str.replace(".", "").replace(",", ".")
                            amount = float(amount_str)

                        # Only include non-zero amounts
                        if amount != 0:
                            result_rows.append({
                                "year": year,
                                "source_type": "plan",
                                "document_id": doc_id,
                                "institution": "Government",
                                "budget_line": budget_line,
                                "amount": amount,
                            })
                    except (ValueError, AttributeError) as e:
                        logger.debug(f"  Skipping value at row {idx}, year {year}: {e}")
                        continue

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
