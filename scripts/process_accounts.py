#!/usr/bin/env python3
"""
Extract budget line items from PDF budget account files into parquet format.
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
LANDING_DIR = PROJECT_DIR / "data" / "landing" / "budget_accounts"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed" / "budget_accounts"

# Create output directory
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def find_pdf_files() -> List[Path]:
    """Find all PDF files in the budget accounts landing directory."""
    return sorted(LANDING_DIR.glob("*/accounts_*.pdf"))


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
        match = re.search(r"accounts_(\d{4})", file_path.name)
        if not match:
            logger.warning(f"Could not parse year from filename: {file_path.name}")
            return None

        year = int(match.group(1))
        doc_id = f"accounts_{year}"

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
                                            "source_type": "accounts",
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


def process_all_accounts() -> None:
    """Process all PDF budget account files."""
    pdf_files = find_pdf_files()

    if not pdf_files:
        logger.warning(f"No PDF files found in {LANDING_DIR}")
        return

    logger.info(f"Found {len(pdf_files)} account files to process")

    processed_count = 0
    for file_path in pdf_files:
        df = extract_from_pdf(file_path)

        if df is not None and not df.empty:
            # Determine output filename
            match = re.search(r"accounts_(\d{4})", file_path.name)
            if match:
                year = match.group(1)
                output_file = PROCESSED_DIR / f"accounts_{year}.parquet"

                # Write to parquet
                df.to_parquet(output_file, compression="snappy")
                logger.info(f"  Saved: {output_file.name}")
                processed_count += 1

    logger.info(f"\nProcessed {processed_count}/{len(pdf_files)} account files successfully")


if __name__ == "__main__":
    process_all_accounts()
