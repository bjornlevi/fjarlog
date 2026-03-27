#!/usr/bin/env python3
"""
Extract budget line items from PDF budget bill files into parquet format.
Part of the silver layer processing.
"""

import logging
from pathlib import Path
from typing import Optional, List
import pandas as pd
import pdfplumber
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


def find_pdf_files() -> List[Path]:
    """Find all PDF files in the budget bills landing directory."""
    return sorted(LANDING_DIR.glob("*/bill_*.pdf"))


def find_xlsx_files() -> List[Path]:
    """Find all XLSX files in the budget bills landing directory."""
    return sorted(LANDING_DIR.glob("*/bill_*.xlsx"))


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
        xls = pd.ExcelFile(file_path)
        logger.info(f"  Sheets: {xls.sheet_names}")

        # Try to find the main data sheet
        data_sheet = None
        for sheet_name in xls.sheet_names:
            if sheet_name.lower() in ["data", "budget", "gögn", "fjárlög", "fjarlög"]:
                data_sheet = sheet_name
                break

        if not data_sheet:
            data_sheet = xls.sheet_names[0]

        logger.info(f"  Using sheet: {data_sheet}")

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


def extract_from_pdf(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract budget data from a PDF file.

    Args:
        file_path: Path to the PDF file

    Returns:
        DataFrame with columns: year, source_type, document_id, institution, budget_line, amount
        Or None if extraction fails
    """
    try:
        logger.info(f"Processing: {file_path.name}")

        # Extract year from filename
        match = re.search(r"bill_(\d{4})", file_path.name)
        if not match:
            logger.warning(f"Could not parse year from filename: {file_path.name}")
            return None

        year = int(match.group(1))
        doc_id = f"bill_{year}"

        result_rows = []

        with pdfplumber.open(file_path) as pdf:
            logger.info(f"  Pages: {len(pdf.pages)}")

            # Extract tables from all pages
            for page_num, page in enumerate(pdf.pages):
                try:
                    tables = page.extract_tables()

                    if tables:
                        logger.info(f"    Page {page_num + 1}: Found {len(tables)} table(s)")

                        for table_idx, table in enumerate(tables):
                            if not table:
                                continue

                            # Try to parse the table
                            try:
                                df_table = pd.DataFrame(table[1:], columns=table[0])

                                # Look for amount columns (numeric values)
                                for idx, row in df_table.iterrows():
                                    # Skip empty rows
                                    if row.isnull().all():
                                        continue

                                    # Try to extract institution and amount
                                    institution = str(row.iloc[0]) if pd.notna(row.iloc[0]) else "Unknown"
                                    budget_line = str(row.iloc[1]) if len(row) > 1 and pd.notna(row.iloc[1]) else "Total"

                                    # Find first numeric value as amount
                                    amount = None
                                    for val in row:
                                        if pd.notna(val):
                                            try:
                                                # Try to convert to float
                                                amount = float(str(val).replace(",", "").replace(".", ""))
                                                break
                                            except (ValueError, AttributeError):
                                                continue

                                    if amount is not None:
                                        result_rows.append({
                                            "year": year,
                                            "source_type": "bill",
                                            "document_id": doc_id,
                                            "institution": institution,
                                            "budget_line": budget_line,
                                            "amount": amount,
                                        })

                            except Exception as e:
                                logger.debug(f"    Could not parse table on page {page_num + 1}: {e}")
                                continue

                except Exception as e:
                    logger.debug(f"  Error extracting tables from page {page_num + 1}: {e}")
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


def process_all_bills() -> None:
    """Process all budget bill files (PDF and XLSX)."""
    pdf_files = find_pdf_files()
    xlsx_files = find_xlsx_files()

    all_files = pdf_files + xlsx_files

    if not all_files:
        logger.warning(f"No bill files found in {LANDING_DIR}")
        return

    logger.info(f"Found {len(all_files)} bill files to process ({len(pdf_files)} PDFs, {len(xlsx_files)} XLSX)")

    processed_count = 0

    # Process PDF files
    for file_path in pdf_files:
        df = extract_from_pdf(file_path)

        if df is not None and not df.empty:
            # Determine output filename
            match = re.search(r"bill_(\d{4})", file_path.name)
            if match:
                year = match.group(1)
                output_file = PROCESSED_DIR / f"bill_{year}.parquet"

                # Write to parquet
                df.to_parquet(output_file, compression="snappy")
                logger.info(f"  Saved: {output_file.name}")
                processed_count += 1

    # Process XLSX files
    for file_path in xlsx_files:
        df = extract_from_xlsx(file_path)

        if df is not None and not df.empty:
            # Determine output filename
            match = re.search(r"bill_(\d{4})", file_path.name)
            if match:
                year = match.group(1)
                output_file = PROCESSED_DIR / f"bill_{year}.parquet"

                # Write to parquet (may overwrite PDF version if both exist)
                df.to_parquet(output_file, compression="snappy")
                logger.info(f"  Saved: {output_file.name}")
                processed_count += 1

    logger.info(f"\nProcessed {processed_count}/{len(all_files)} bill files successfully")


if __name__ == "__main__":
    process_all_bills()
