#!/usr/bin/env python3
"""
Scrape budget plans (Tillaga til fjármálaáætlunar) from stjornarradid.is.
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
PLANS_INDEX_URL = f"{BASE_URL}/verkefni/opinber-fjarmal/fjarmalaaaetlun/"

# Plan periods to scrape
PLAN_PERIODS = [
    (2027, 2031),
    (2026, 2030),
    (2025, 2029),
    (2024, 2028),
    (2023, 2027),
    (2022, 2026),
    (2021, 2025),
    (2020, 2024),
]


def get_plan_page_url(start_year: int, end_year: int) -> str:
    """Get the URL for a specific budget plan period page."""
    return f"{BASE_URL}/verkefni/opinber-fjarmal/fjarmalaaaetlun/fjarmalaaaetlun-{start_year}-{end_year}/"


def extract_documents_from_page(start_year: int, end_year: int) -> List[Dict]:
    """
    Extract budget plan documents from a period page.

    Args:
        start_year: Start year of the plan period
        end_year: End year of the plan period

    Returns:
        List of document dictionaries
    """
    url = get_plan_page_url(start_year, end_year)
    documents = []

    try:
        logger.info(f"Fetching budget plan {start_year}-{end_year}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, "html.parser")

        # Look for download links (typically XLSX or Excel files for budget plans)
        for link in soup.find_all("a"):
            href = link.get("href", "")
            text = link.get_text(strip=True)

            if not href or not text:
                continue

            # Look for Excel/XLSX links or any document links with "tillaga" or "áætlun"
            is_excel = any(
                ext in href.lower() for ext in [".xlsx", ".xls", ".excel"]
            )
            is_plan_doc = any(
                keyword in text.lower()
                for keyword in ["tillaga", "áætlun", "fjármála", "fjarmala"]
            )

            if is_excel or is_plan_doc:
                # Make absolute URL if relative
                if href.startswith(("http://", "https://")):
                    full_url = href
                else:
                    full_url = urljoin(BASE_URL, href)

                # Determine format
                fmt = "xlsx" if is_excel else "pdf"

                logger.info(
                    f"Found {start_year}-{end_year}: {text} -> {full_url}"
                )
                documents.append(
                    {
                        "title": text,
                        "year": start_year,
                        "period": (start_year, end_year),
                        "url": full_url,
                        "format": fmt,
                    }
                )

        # Fallback: look for any Excel links
        if not documents:
            for link in soup.find_all("a", href=re.compile(r"\.(xlsx?|xls)$", re.I)):
                href = link.get("href", "")
                if href:
                    full_url = urljoin(BASE_URL, href)
                    text = (
                        link.get_text(strip=True)
                        or f"Tillaga til fjármálaáætlunar {start_year}-{end_year}"
                    )
                    fmt = "xlsx" if "xlsx" in href.lower() else "xls"
                    logger.info(
                        f"Found {start_year}-{end_year} (fallback): {text} -> {full_url}"
                    )
                    documents.append(
                        {
                            "title": text,
                            "year": start_year,
                            "period": (start_year, end_year),
                            "url": full_url,
                            "format": fmt,
                        }
                    )

    except requests.RequestException as e:
        logger.warning(f"Failed to fetch {start_year}-{end_year} page: {e}")

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

    # Index existing documents by year
    if "budget_plans" not in data_sources:
        data_sources["budget_plans"] = {
            "description": "Tillaga til fjármálaáætlunar (Budget plans/estimates)",
            "source": "https://www.stjornarradid.is/verkefni/opinber-fjarmal/fjarmalaaaetlun/",
            "documents": [],
        }

    existing_by_year = {doc["year"]: doc for doc in data_sources["budget_plans"]["documents"]}

    # Update with found documents
    updated_count = 0
    for doc in documents:
        year = doc["year"]
        start_year, end_year = doc["period"]
        doc_id = f"plan_{start_year}_{end_year}"

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
                "format": doc["format"],
                "status": "active",
            }
            data_sources["budget_plans"]["documents"].append(new_doc)
            logger.info(f"Added: {doc_id}")
            updated_count += 1

    # Save
    try:
        with open(DATA_SOURCES_FILE, "w") as f:
            json.dump(data_sources, f, indent=2, ensure_ascii=False)
        logger.info(f"Updated data_sources.json with {updated_count} plan documents")
    except IOError as e:
        logger.error(f"Failed to write data_sources.json: {e}")


def main():
    """Main scraping function."""
    logger.info("Starting budget plans (fjármálaáætlun) scraper...")
    all_documents = []

    for start_year, end_year in PLAN_PERIODS:
        docs = extract_documents_from_page(start_year, end_year)
        all_documents.extend(docs)

    if all_documents:
        logger.info(f"Found {len(all_documents)} plan documents")
        update_data_sources(all_documents)
    else:
        logger.warning("No plan documents found")


if __name__ == "__main__":
    main()
