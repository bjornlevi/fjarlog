#!/usr/bin/env python3
"""
Run all data source scrapers to populate data_sources.json.
Orchestrates scrapers for budget bills, plans, and accounts.
"""

import asyncio
import subprocess
import sys
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent

SCRAPERS = [
    ("Budget Bills (Frumvarp til fjárlaga)", "scrape_fjarlog.py"),
    ("Budget Plans (Tillaga til fjármálaáætlunar)", "scrape_fjarmalaaaetlun.py"),
    # ("Budget Accounts (Ríkisreikningur)", "scrape_rikisreikningur.py"),  # Skipped - URLs not available via automation
]


def run_scraper(name: str, script: str) -> bool:
    """
    Run a single scraper.

    Args:
        name: Scraper name for logging
        script: Script filename

    Returns:
        True if successful, False otherwise
    """
    script_path = SCRIPT_DIR / script
    logger.info(f"\n{'='*60}")
    logger.info(f"Running: {name}")
    logger.info(f"{'='*60}")

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(SCRIPT_DIR),
            timeout=300,  # 5 minute timeout per scraper
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        logger.error(f"Scraper timed out: {name}")
        return False
    except Exception as e:
        logger.error(f"Failed to run {name}: {e}")
        return False


def main():
    """Run all scrapers."""
    logger.info("Starting all data source scrapers...")
    results = {}

    for name, script in SCRAPERS:
        success = run_scraper(name, script)
        results[name] = success

    # Summary
    logger.info("\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)

    successful = sum(1 for v in results.values() if v)
    total = len(results)

    for name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{status}: {name}")

    logger.info(f"\nTotal: {successful}/{total} scrapers completed successfully")
    logger.info("="*60)

    if successful == total:
        logger.info("\nAll data sources populated! Run 'python scripts/download_sources.py' to download documents.")
        return 0
    else:
        logger.warning(f"\n{total - successful} scraper(s) failed. Check logs above for details.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
