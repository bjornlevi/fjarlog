#!/usr/bin/env python3
"""
Scrape budget accounts (ríkisreikningur) from rikisreikningur.is using Playwright.
Extracts document URLs and updates data_sources.json.
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import List, Dict
from playwright.async_api import async_playwright
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
DATA_SOURCES_FILE = PROJECT_DIR / "data_sources.json"

URL = "https://rikisreikningur.is/utgefin-gogn"


async def extract_documents() -> List[Dict]:
    """
    Scrape budget accounts documents from the website.
    Looks for pattern: "Ríkisreikningur gögn árið [YEAR]" with links "Sækja csv skrá" and "Sækja xlsx skrá"

    Returns:
        List of document dictionaries with title, year, and url
    """
    documents = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            logger.info(f"Loading {URL}...")
            await page.goto(URL, wait_until="networkidle")

            # Wait for content to load
            await page.wait_for_selector("a", timeout=10000)

            logger.info("Page loaded, extracting documents...")

            # Get page content as text to find year patterns
            content = await page.content()

            # Get all links and their text
            all_links = await page.locator("a").all()
            logger.info(f"Found {len(all_links)} links on page")

            # Build a list of (href, text) tuples
            link_data = []
            for link in all_links:
                try:
                    href = await link.get_attribute("href")
                    text = await link.text_content()
                    if href and text:
                        link_data.append((href.strip(), text.strip().lower()))
                except Exception:
                    continue

            logger.info(f"Extracted {len(link_data)} links with URLs and text")

            # Find links for "Sækja csv skrá" and "Sækja xlsx skrá"
            csv_links = [(href, text) for href, text in link_data if "csv" in text and "sækja" in text]
            xlsx_links = [(href, text) for href, text in link_data if "xlsx" in text and "sækja" in text]

            logger.info(f"Found {len(csv_links)} CSV links and {len(xlsx_links)} XLSX links")

            # Find all year headers and their associated links
            year_matches = list(re.finditer(r"Ríkisreikningur\s+gögn\s+árið\s+(\d{4})", content, re.IGNORECASE))

            for idx, year_match in enumerate(year_matches):
                year = int(year_match.group(1))

                # Filter for reasonable years (2015-2025)
                if year < 2015 or year > 2025:
                    continue

                logger.info(f"Processing year: {year}")

                # CSV and XLSX links should be near this year in the HTML
                # Use index to find the corresponding links (they come after the year text)
                # We'll add both CSV and XLSX if available
                if len(csv_links) > idx:
                    href, text = csv_links[idx]
                    logger.info(f"Found: Ríkisreikningur {year} (CSV) -> {href}")
                    documents.append(
                        {
                            "title": f"Ríkisreikningur gögn {year} (CSV)",
                            "year": year,
                            "url": href,
                            "format": "csv",
                        }
                    )

                if len(xlsx_links) > idx:
                    href, text = xlsx_links[idx]
                    logger.info(f"Found: Ríkisreikningur {year} (XLSX) -> {href}")
                    documents.append(
                        {
                            "title": f"Ríkisreikningur gögn {year} (XLSX)",
                            "year": year,
                            "url": href,
                            "format": "xlsx",
                        }
                    )

        except Exception as e:
            logger.error(f"Error loading page: {e}")

        finally:
            await browser.close()

    return documents


def update_data_sources(documents: List[Dict]) -> None:
    """
    Update data_sources.json with extracted document URLs.

    Args:
        documents: List of document dictionaries from scraper (CSV and XLSX for each year)
    """
    try:
        with open(DATA_SOURCES_FILE, "r") as f:
            data_sources = json.load(f)
    except FileNotFoundError:
        logger.error(f"Data sources file not found: {DATA_SOURCES_FILE}")
        return

    # Group documents by year (may have CSV and XLSX for same year)
    docs_by_year = {}
    for doc in documents:
        year = doc["year"]
        if year not in docs_by_year:
            docs_by_year[year] = []
        docs_by_year[year].append(doc)

    # Update budget_accounts section
    if "budget_accounts" not in data_sources:
        data_sources["budget_accounts"] = {
            "description": "Ríkisreikningur (Budget accounts/statements)",
            "source": "https://rikisreikningur.is/utgefin-gogn",
            "documents": [],
        }

    # Update or add document entries
    existing_by_id = {doc["id"]: doc for doc in data_sources["budget_accounts"]["documents"]}
    updated_count = 0

    for year in sorted(docs_by_year.keys(), reverse=True):
        year_docs = docs_by_year[year]

        # Prefer XLSX over CSV, but keep both if available
        for doc in year_docs:
            fmt = doc.get("format", "unknown")
            doc_id = f"accounts_{year}_{fmt}"

            if doc_id in existing_by_id:
                # Update existing entry
                existing_by_id[doc_id]["url"] = doc["url"]
                existing_by_id[doc_id]["title"] = doc["title"]
                logger.info(f"Updated: {doc_id} with URL: {doc['url']}")
                updated_count += 1
            else:
                # Create new entry
                new_doc = {
                    "id": doc_id,
                    "year": year,
                    "title": doc["title"],
                    "url": doc["url"],
                    "format": fmt,
                    "status": "active",
                }
                data_sources["budget_accounts"]["documents"].append(new_doc)
                logger.info(f"Added: {doc_id} with URL: {doc['url']}")
                updated_count += 1

    # Save updated data sources
    try:
        with open(DATA_SOURCES_FILE, "w") as f:
            json.dump(data_sources, f, indent=2, ensure_ascii=False)
        logger.info(f"Updated data_sources.json with {updated_count} documents")
    except IOError as e:
        logger.error(f"Failed to write data_sources.json: {e}")


async def main():
    """Main scraping function."""
    logger.info("Starting ríkisreikningur.is scraper...")
    documents = await extract_documents()

    if documents:
        logger.info(f"Found {len(documents)} documents")
        update_data_sources(documents)
    else:
        logger.warning("No documents found")


if __name__ == "__main__":
    asyncio.run(main())
