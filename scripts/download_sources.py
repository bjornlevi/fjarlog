#!/usr/bin/env python3
"""
Download budget documents from data sources into the landing zone.
Part of the medallion data pipeline.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List
import requests
from datetime import datetime

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
LANDING_DIR = PROJECT_DIR / "data" / "landing"


def load_data_sources() -> Dict:
    """Load the data sources configuration."""
    try:
        with open(DATA_SOURCES_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Data sources file not found: {DATA_SOURCES_FILE}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in data sources file: {e}")
        raise


def download_file(url: str, dest_path: Path, timeout: int = 30) -> bool:
    """
    Download a file from URL to destination.

    Args:
        url: Remote file URL
        dest_path: Local destination path
        timeout: Request timeout in seconds

    Returns:
        True if successful, False otherwise
    """
    if not url:
        logger.warning(f"Empty URL provided, skipping")
        return False

    try:
        logger.info(f"Downloading: {url}")
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()

        # Create parent directories if needed
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        logger.info(f"Successfully saved to: {dest_path}")
        return True

    except requests.RequestException as e:
        logger.error(f"Failed to download {url}: {e}")
        return False
    except IOError as e:
        logger.error(f"Failed to write file {dest_path}: {e}")
        return False


def download_all_sources(data_sources: Dict) -> None:
    """Download all documents from data sources."""
    stats = {'success': 0, 'skipped': 0, 'failed': 0}

    for doc_type, type_info in data_sources.items():
        logger.info(f"\nProcessing {doc_type}: {type_info.get('description', 'N/A')}")

        # Create type-specific directory
        type_dir = LANDING_DIR / doc_type

        for doc in type_info.get('documents', []):
            doc_id = doc.get('id', 'unknown')
            url = doc.get('url', '').strip()
            year = doc.get('year', 'unknown')
            fmt = doc.get('format', 'unknown')

            if not url:
                logger.info(f"  Skipping {doc_id}: No URL provided")
                stats['skipped'] += 1
                continue

            # Create filename: {id}_{year}.{format}
            filename = f"{doc_id}_{year}.{fmt}"
            dest_path = type_dir / str(year) / filename

            if download_file(url, dest_path):
                stats['success'] += 1
            else:
                stats['failed'] += 1

    # Summary
    logger.info("\n" + "="*50)
    logger.info(f"Download Summary:")
    logger.info(f"  Successfully downloaded: {stats['success']}")
    logger.info(f"  Skipped (no URL): {stats['skipped']}")
    logger.info(f"  Failed: {stats['failed']}")
    logger.info("="*50)


if __name__ == "__main__":
    try:
        data_sources = load_data_sources()
        download_all_sources(data_sources)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)
