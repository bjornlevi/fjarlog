#!/usr/bin/env python3
"""
Extract budget line items from CSV/XLSX budget bill data files into parquet format.
Part of the silver layer processing.

Note: Budget bills are provided as structured data files (CSV/XLSX) with numerical data,
not as PDFs with tables.
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
LANDING_DIR = PROJECT_DIR / "data" / "landing" / "budget_bills"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed" / "budget_bills"

# Create output directory
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def find_csv_files() -> List[Path]:
    """Find all CSV files in the budget bills landing directory."""
    return sorted(LANDING_DIR.glob("*/*.csv"))


def find_xlsx_files() -> List[Path]:
    """Find all XLSX files in the budget bills landing directory."""
    return sorted(LANDING_DIR.glob("*/*.xlsx"))


def extract_from_xlsx(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract budget data from an XLSX budget bill file.

    Args:
        file_path: Path to the XLSX file

    Returns:
        DataFrame with columns: year, source_type, document_id, institution, budget_line, amount
        Or None if extraction fails
    """
    try:
        logger.info(f"Processing XLSX: {file_path.name}")

        # Extract year from filename
        match = re.search(r"bill_(\d{4})", file_path.name)
        if not match:
            logger.warning(f"Could not parse year from filename: {file_path.name}")
            return None

        year = int(match.group(1))
        doc_id = f"bill_{year}"

        # Read Excel file
        xls = pd.ExcelFile(file_path, engine='openpyxl')
        logger.info(f"  Sheets: {xls.sheet_names}")

        # Skip table of contents sheet and find the main data sheet
        data_sheet = None
        for sheet_name in xls.sheet_names:
            sheet_lower = sheet_name.lower()
            if sheet_lower not in ["töfluyfirlit", "oversigt", "index"]:
                if sheet_lower in ["data", "budget", "gögn", "fjárlög", "fjarlög"]:
                    data_sheet = sheet_name
                    break
                elif data_sheet is None:
                    data_sheet = sheet_name

        if not data_sheet and xls.sheet_names:
            data_sheet = xls.sheet_names[0]

        logger.info(f"  Using sheet: {data_sheet}")

        # Read the sheet
        df = pd.read_excel(file_path, sheet_name=data_sheet, engine='openpyxl')

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
                        if isinstance(val, (int, float)):
                            amount = float(val)
                        else:
                            amount_str = str(val).strip()
                            amount_str = amount_str.replace(".", "").replace(",", ".")
                            amount = float(amount_str)
                        if amount != 0:
                            break
                    except (ValueError, AttributeError):
                        continue

            if amount is not None and amount != 0:
                result_rows.append({
                    "year": year,
                    "source_type": "bill",
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


def extract_from_csv(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract budget data from a CSV file (semicolon-delimited Icelandic budget data).

    Columns: Ár, Afurð, FlokkunNy, Málefnasvið, Málaflokkur, Ráðuneyti, Liður, Viðfang, TegundNota, Upphæð

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
        doc_id = f"bill_{year}"

        # Read CSV file with semicolon delimiter
        df = pd.read_csv(file_path, sep=";", encoding="utf-8-sig")

        logger.info(f"  Shape: {df.shape}")
        logger.info(f"  Columns: {df.columns.tolist()}")

        result_rows = []

        # Expected columns: Ár, Afurð, FlokkunNy, Málefnasvið, Málaflokkur, Ráðuneyti, Liður, Viðfang, TegundNota, Upphæð
        if len(df.columns) < 10:
            logger.warning(f"  CSV has unexpected structure: {len(df.columns)} columns")
            return None

        # Extract rows
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isnull().all():
                continue

            try:
                # Column indices (0-based)
                # 0: Ár (Year), 5: Ráðuneyti (Ministry), 4: Málaflokkur (Category), 9: Upphæð (Amount)
                row_year = int(row.iloc[0]) if pd.notna(row.iloc[0]) else None
                institution = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else "Unknown"
                budget_line = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else str(row.iloc[8]).strip()

                # Parse amount - handle Icelandic number format (comma as decimal, period as thousands)
                amount_str = str(row.iloc[9]).strip() if pd.notna(row.iloc[9]) else "0"
                amount_str = amount_str.replace(".", "").replace(",", ".")
                amount = float(amount_str)

                # Only include positive amounts (exclude zeros and negative values)
                if amount > 0 and row_year == year:
                    result_rows.append({
                        "year": year,
                        "source_type": "bill",
                        "document_id": doc_id,
                        "institution": institution,
                        "budget_line": budget_line,
                        "amount": amount,
                    })
            except (ValueError, IndexError) as e:
                logger.debug(f"  Skipping row {idx}: {e}")
                continue

        if result_rows:
            logger.info(f"  Extracted {len(result_rows)} records")
            return pd.DataFrame(result_rows)
        else:
            logger.warning(f"  No records extracted from {file_path.name}")
            return None

    except Exception as e:
        logger.error(f"Error processing CSV {file_path.name}: {e}")
        return None


def process_all_bills() -> None:
    """Process all budget bill data files (CSV and XLSX)."""
    csv_files = find_csv_files()
    xlsx_files = find_xlsx_files()

    all_files = csv_files + xlsx_files

    if not all_files:
        logger.warning(f"No bill data files found in {LANDING_DIR}")
        return

    logger.info(f"Found {len(all_files)} bill data files to process ({len(csv_files)} CSV, {len(xlsx_files)} XLSX)")

    processed_count = 0
    processed_years = set()

    # Process CSV files
    for file_path in csv_files:
        df = extract_from_csv(file_path)

        if df is not None and not df.empty:
            year = df["year"].iloc[0]
            if year not in processed_years:
                output_file = PROCESSED_DIR / f"bill_{year}.parquet"
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
                output_file = PROCESSED_DIR / f"bill_{year}.parquet"
                df.to_parquet(output_file, compression="snappy")
                logger.info(f"  Saved: {output_file.name}")
                processed_count += 1
                processed_years.add(year)

    logger.info(f"\nProcessed {processed_count} bill years successfully")


if __name__ == "__main__":
    process_all_bills()
