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
            logger.info(f"Found {len(all_links)} total links on page")

            # Build a list of (href, text) tuples - log ALL links for debugging
            link_data = []
            for idx, link in enumerate(all_links):
                try:
                    href = await link.get_attribute("href")
                    text = await link.text_content()
                    text_clean = text.strip() if text else ""

                    # Log all links with their content
                    if text_clean:
                        logger.info(f"  [{idx}] Text: '{text_clean}' | URL: {href}")

                    if href and text_clean:
                        link_data.append((href.strip(), text_clean))
                except Exception as e:
                    logger.debug(f"Error processing link {idx}: {e}")
                    continue

            logger.info(f"Extracted {len(link_data)} links with both text and URLs")

            # Find links for "Sækja csv skrá" and "Sækja xlsx skrá"
            # Check both href and text for CSV/XLSX keywords
            csv_links = [href for href, text in link_data if "csv" in href.lower() or "csv" in text.lower()]
            xlsx_links = [href for href, text in link_data if ("xlsx" in href.lower() or "xls" in href.lower()) or ("xlsx" in text.lower() or "xls" in text.lower())]

            logger.info(f"Found {len(csv_links)} CSV links: {csv_links[:3]}")
            logger.info(f"Found {len(xlsx_links)} XLSX links: {xlsx_links[:3]}")

            # Find all year headers
            year_matches = list(re.finditer(r"Ríkisreikningur\s+gögn\s+árið\s+(\d{4})", content, re.IGNORECASE))
            logger.info(f"Found {len(year_matches)} year entries")

            for idx, year_match in enumerate(year_matches):
                year = int(year_match.group(1))

                # Filter for reasonable years (2015-2025)
                if year < 2015 or year > 2025:
                    continue

                logger.info(f"Processing year: {year} (index {idx})")

                # Match CSV link (should be at index idx*2)
                csv_idx = idx * 2
                if csv_idx < len(csv_links):
                    href = csv_links[csv_idx]
                    logger.info(f"  CSV [{csv_idx}]: {href}")
                    documents.append(
                        {
                            "title": f"Ríkisreikningur gögn {year} (CSV)",
                            "year": year,
                            "url": href,
                            "format": "csv",
                        }
                    )
                else:
                    logger.warning(f"  No CSV link found for index {csv_idx}")

                # Match XLSX link (should be at index idx*2+1)
                xlsx_idx = idx * 2 + 1
                if xlsx_idx < len(xlsx_links):
                    href = xlsx_links[xlsx_idx]
                    logger.info(f"  XLSX [{xlsx_idx}]: {href}")
                    documents.append(
                        {
                            "title": f"Ríkisreikningur gögn {year} (XLSX)",
                            "year": year,
                            "url": href,
                            "format": "xlsx",
                        }
                    )
                else:
                    logger.warning(f"  No XLSX link found for index {xlsx_idx}")

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
