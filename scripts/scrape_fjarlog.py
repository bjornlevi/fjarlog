#!/usr/bin/env python3
"""
Scrape budget bills (frumvarp til fjárlaga) from stjornarradid.is.
Extracts document URLs and updates data_sources.json.
"""

import json
import logging
from pathlib import Path
from typing import List, Dict
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DATA_SOURCES_FILE = PROJECT_DIR / "data_sources.json"

BASE_URL = "https://www.stjornarradid.is"
BILLS_INDEX_URL = f"{BASE_URL}/verkefni/opinber-fjarmal/fjarlog/"

# Year pages to scrape
BILL_YEARS = list(range(2026, 2017, -1))  # 2026 down to 2018


def get_bill_page_url(year: int) -> str:
    """Get the URL for a specific budget bill year page."""
    return f"{BASE_URL}/verkefni/opinber-fjarmal/fjarlog/fjarlog-fyrir-arid-{year}/"


def extract_documents_from_page(year: int) -> List[Dict]:
    """
    Extract budget bill documents from a year page.

    Args:
        year: The budget year

    Returns:
        List of document dictionaries
    """
    url = get_bill_page_url(year)
    documents = []

    try:
        logger.info(f"Fetching budget bill for {year}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Look for download links (PDFs and XLSX for budget bills)
        for link in soup.find_all("a"):
            href = link.get("href", "")
            text = link.get_text(strip=True)

            if not href or not text:
                continue

            # Determine format
            fmt = None
            if href.lower().endswith(".pdf") or "pdf" in href.lower():
                fmt = "pdf"
            elif href.lower().endswith((".xlsx", ".xls")) or "excel" in href.lower():
                fmt = "xlsx"

            if not fmt:
                continue

            # Make absolute URL if relative
            if href.startswith(("http://", "https://")):
                full_url = href
            else:
                full_url = urljoin(BASE_URL, href)

            # Check if it looks like a budget bill document
            if any(
                keyword in text.lower()
                for keyword in ["frumvarp", "fjárlög", "fjarlög", "budget", "fylgirit"]
            ):
                logger.info(f"Found {year} ({fmt}): {text} -> {full_url}")
                documents.append(
                    {
                        "title": text,
                        "year": year,
                        "url": full_url,
                        "format": fmt,
                    }
                )

        # Also look for any document links in the page (fallback)
        if not documents:
            for link in soup.find_all("a", href=re.compile(r"\.(pdf|xlsx?)$", re.I)):
                href = link.get("href", "")
                if href:
                    fmt = "xlsx" if "xlsx" in href.lower() else "pdf"
                    full_url = urljoin(BASE_URL, href)
                    text = link.get_text(strip=True) or f"Frumvarp til fjárlaga {year}"
                    logger.info(f"Found {year} ({fmt}, fallback): {text} -> {full_url}")
                    documents.append(
                        {
                            "title": text,
                            "year": year,
                            "url": full_url,
                            "format": fmt,
                        }
                    )

    except requests.RequestException as e:
        logger.warning(f"Failed to fetch {year} page: {e}")

    return documents


def update_data_sources(documents: List[Dict]) -> None:
    """
    Update data_sources.json with extracted document URLs.

    Args:
        documents: List of document dictionaries
    """
    try:
        with open(DATA_SOURCES_FILE, "r") as f:
            data_sources = json.load(f)
    except FileNotFoundError:
        logger.error(f"Data sources file not found: {DATA_SOURCES_FILE}")
        return

    # Index existing documents by ID
    if "budget_bills" not in data_sources:
        data_sources["budget_bills"] = {
            "description": "Frumvarp til fjárlaga (Budget bills/proposals)",
            "source": "https://www.stjornarradid.is/verkefni/opinber-fjarmal/fjarlog/",
            "documents": [],
        }

    existing_by_year = {doc["year"]: doc for doc in data_sources["budget_bills"]["documents"]}

    # Update with found documents
    updated_count = 0
    for doc in documents:
        year = doc["year"]
        doc_id = f"bill_{year}"

        if year in existing_by_year:
            # Update existing
            existing_by_year[year]["url"] = doc["url"]
            existing_by_year[year]["title"] = doc["title"]
            logger.info(f"Updated: {doc_id}")
            updated_count += 1
        else:
            # Add new
            new_doc = {
                "id": doc_id,
                "year": year,
                "title": doc["title"],
                "url": doc["url"],
                "format": "pdf",
                "status": "active",
            }
            data_sources["budget_bills"]["documents"].append(new_doc)
            logger.info(f"Added: {doc_id}")
            updated_count += 1

    # Save
    try:
        with open(DATA_SOURCES_FILE, "w") as f:
            json.dump(data_sources, f, indent=2, ensure_ascii=False)
        logger.info(f"Updated data_sources.json with {updated_count} bill documents")
    except IOError as e:
        logger.error(f"Failed to write data_sources.json: {e}")


def main():
    """Main scraping function."""
    logger.info("Starting budget bills (fjárlög) scraper...")
    all_documents = []

    for year in BILL_YEARS:
        docs = extract_documents_from_page(year)
        all_documents.extend(docs)

    if all_documents:
        logger.info(f"Found {len(all_documents)} bill documents")
        update_data_sources(all_documents)
    else:
        logger.warning("No bill documents found")


if __name__ == "__main__":
    main()
