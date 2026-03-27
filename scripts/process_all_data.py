#!/usr/bin/env python3
"""
Orchestrate all data processing steps: plans → bills → accounts → comparison.
"""

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

PROCESSING_STEPS = [
    ("Budget Plans", "process_plans.py"),
    ("Budget Bills", "process_bills.py"),
    # ("Budget Accounts", "process_accounts.py"),  # Skipped - URLs not available via automated scraping
    ("Build Comparison", "build_comparison.py"),
]


def run_step(name: str, script: str) -> bool:
    """
    Run a single processing step.

    Args:
        name: Step name for logging
        script: Script filename

    Returns:
        True if successful, False otherwise
    """
    script_path = SCRIPT_DIR / script
    logger.info(f"\n{'='*60}")
    logger.info(f"Step: {name}")
    logger.info(f"{'='*60}")

    try:
        result = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(SCRIPT_DIR),
            timeout=600,  # 10 minute timeout per step
        )
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        logger.error(f"Step timed out: {name}")
        return False
    except Exception as e:
        logger.error(f"Failed to run {name}: {e}")
        return False


def main():
    """Run all processing steps."""
    logger.info("Starting data processing pipeline...")
    results = {}

    for name, script in PROCESSING_STEPS:
        success = run_step(name, script)
        results[name] = success

        # Stop if any step fails
        if not success:
            logger.warning(f"Step failed: {name}. Stopping pipeline.")
            break

    # Summary
    logger.info("\n" + "="*60)
    logger.info("SUMMARY")
    logger.info("="*60)

    successful = sum(1 for v in results.values() if v)
    total = len(results)

    for name, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED"
        logger.info(f"{status}: {name}")

    logger.info(f"\nCompleted: {successful}/{total} steps")
    logger.info("="*60)

    return 0 if successful == total else 1


if __name__ == "__main__":
    sys.exit(main())
