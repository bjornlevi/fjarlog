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

            # Wait for content to load (adjust selector as needed)
            await page.wait_for_selector("a", timeout=10000)

            logger.info("Page loaded, extracting documents...")

            # Get all links on the page
            links = await page.locator("a").all()

            for link in links:
                try:
                    href = await link.get_attribute("href")
                    text = await link.text_content()

                    if not href or not text:
                        continue

                    text = text.strip()

                    # Look for patterns like "Ríkisreikningur 2023" or "2023"
                    year_match = re.search(r"\b(20\d{2})\b", text)
                    if not year_match:
                        continue

                    year = int(year_match.group(1))

                    # Filter for reasonable years (1995-2024)
                    if year < 1995 or year > 2024:
                        continue

                    # Check if it looks like a document link
                    if any(
                        keyword in text.lower()
                        for keyword in ["ríkisreikningur", "reikningur", "árs"]
                    ):
                        logger.info(f"Found: {text} -> {href}")
                        documents.append(
                            {
                                "title": text,
                                "year": year,
                                "url": href,
                                "text": text,
                            }
                        )

                except Exception as e:
                    logger.debug(f"Error processing link: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error loading page: {e}")

        finally:
            await browser.close()

    return documents


def update_data_sources(documents: List[Dict]) -> None:
    """
    Update data_sources.json with extracted document URLs.

    Args:
        documents: List of document dictionaries from scraper
    """
    try:
        with open(DATA_SOURCES_FILE, "r") as f:
            data_sources = json.load(f)
    except FileNotFoundError:
        logger.error(f"Data sources file not found: {DATA_SOURCES_FILE}")
        return

    # Group documents by year
    docs_by_year = {}
    for doc in documents:
        year = doc["year"]
        if year not in docs_by_year:
            docs_by_year[year] = doc

    # Update budget_accounts section
    if "budget_accounts" not in data_sources:
        data_sources["budget_accounts"] = {
            "description": "Ríkisreikningur (Budget accounts/statements)",
            "source": "https://rikisreikningur.is/utgefin-gogn",
            "documents": [],
        }

    # Update or add document entries
    existing_ids = {doc["id"]: doc for doc in data_sources["budget_accounts"]["documents"]}
    updated_count = 0

    for year, doc in sorted(docs_by_year.items(), reverse=True):
        doc_id = f"accounts_{year}"

        if doc_id in existing_ids:
            # Update existing entry
            existing_ids[doc_id]["url"] = doc["url"]
            existing_ids[doc_id]["title"] = doc["title"]
            logger.info(f"Updated: {doc_id} with URL: {doc['url']}")
            updated_count += 1
        else:
            # Create new entry
            new_doc = {
                "id": doc_id,
                "year": year,
                "title": doc["title"],
                "url": doc["url"],
                "format": "pdf",
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
