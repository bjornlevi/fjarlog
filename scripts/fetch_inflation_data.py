#!/usr/bin/env python3
"""
Fetch inflation indices from Statistics Iceland (Hagstofa Íslands).
- Consumer Price Index (CPI)
- Wage Index

Data is stored as a JSON file for use in inflation adjustment calculations.
"""

import logging
import json
import requests
from pathlib import Path
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
DATA_DIR = PROJECT_DIR / "data"

# Create output directory
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Output file
INFLATION_FILE = DATA_DIR / "inflation_indices.json"

# API endpoints
CPI_API = "https://px.hagstofa.is:443/pxis/api/v1/is/Efnahagur/visitolur/1_vnv/1_vnv/VIS01000.px"
WAGE_API = "https://px.hagstofa.is:443/pxis/api/v1/is/Samfelag/launogtekjur/2_lvt/1_manadartolur/LAU04000.px"


def fetch_cpi_data():
    """Fetch Consumer Price Index year-over-year change percentages."""
    logger.info("Fetching Consumer Price Index data...")

    cpi_data = {}

    # Fetch data for years 2015-2026
    for year in range(2015, 2027):
        for month in range(1, 13):
            month_str = f"{year}M{month:02d}"

            payload = {
                "query": [
                    {
                        "code": "Mánuður",
                        "selection": {
                            "filter": "item",
                            "values": [month_str]
                        }
                    },
                    {
                        "code": "Vísitala",
                        "selection": {
                            "filter": "item",
                            "values": ["CPI"]
                        }
                    },
                    {
                        "code": "Liður",
                        "selection": {
                            "filter": "item",
                            "values": ["change_A"]
                        }
                    }
                ],
                "response": {
                    "format": "json"
                }
            }

            try:
                response = requests.post(CPI_API, json=payload, timeout=10)
                response.raise_for_status()
                data = response.json()

                # Extract the value from response
                if data.get("data") and len(data["data"]) > 0:
                    value = data["data"][0].get("values", [None])[0]
                    if value is not None:
                        cpi_data[month_str] = float(value)
                        logger.debug(f"  {month_str}: {value}%")
            except requests.RequestException as e:
                logger.warning(f"  Failed to fetch {month_str}: {e}")
                continue

    logger.info(f"Fetched {len(cpi_data)} CPI data points")
    return cpi_data


def fetch_wage_data():
    """Fetch Wage Index year-over-year change percentages."""
    logger.info("Fetching Wage Index data...")

    wage_data = {}

    # Fetch data for years 2015-2026
    for year in range(2015, 2027):
        for month in range(1, 13):
            month_str = f"{year}M{month:02d}"

            payload = {
                "query": [
                    {
                        "code": "Mánuður",
                        "selection": {
                            "filter": "item",
                            "values": [month_str]
                        }
                    },
                    {
                        "code": "Eining",
                        "selection": {
                            "filter": "item",
                            "values": ["change_A"]
                        }
                    }
                ],
                "response": {
                    "format": "json"
                }
            }

            try:
                response = requests.post(WAGE_API, json=payload, timeout=10)
                response.raise_for_status()
                data = response.json()

                # Extract the value from response
                if data.get("data") and len(data["data"]) > 0:
                    value = data["data"][0].get("values", [None])[0]
                    if value is not None:
                        wage_data[month_str] = float(value)
                        logger.debug(f"  {month_str}: {value}%")
            except requests.RequestException as e:
                logger.warning(f"  Failed to fetch {month_str}: {e}")
                continue

    logger.info(f"Fetched {len(wage_data)} wage data points")
    return wage_data


def get_yearly_average(monthly_data):
    """Calculate yearly average from monthly data."""
    yearly_avg = {}

    for month_str, value in monthly_data.items():
        year = int(month_str.split('M')[0])

        if year not in yearly_avg:
            yearly_avg[year] = []
        yearly_avg[year].append(value)

    # Calculate averages
    result = {}
    for year, values in yearly_avg.items():
        result[year] = sum(values) / len(values) if values else None

    return result


def main():
    """Fetch and save inflation data."""
    logger.info("Fetching inflation indices from Hagstofa Íslands...")

    cpi_monthly = fetch_cpi_data()
    wage_monthly = fetch_wage_data()

    # Calculate yearly averages
    cpi_yearly = get_yearly_average(cpi_monthly)
    wage_yearly = get_yearly_average(wage_monthly)

    # Compile inflation data
    inflation_data = {
        "fetched_at": datetime.now().isoformat(),
        "source": {
            "cpi": "Hagstofa Íslands - Consumer Price Index (Vísitala neysluverðs)",
            "wage": "Hagstofa Íslands - Wage Index (Vinnukaupavísitala)"
        },
        "methodology": {
            "cpi_wage_split": {
                "wages": 0.35,
                "cpi": 0.65,
                "notes": "Assumption: ~35% of government budget is wages, ~65% is other costs affected by CPI"
            }
        },
        "cpi": {
            "monthly": cpi_monthly,
            "yearly": cpi_yearly
        },
        "wage": {
            "monthly": wage_monthly,
            "yearly": wage_yearly
        }
    }

    # Save to JSON
    with open(INFLATION_FILE, 'w', encoding='utf-8') as f:
        json.dump(inflation_data, f, indent=2, ensure_ascii=False)

    logger.info(f"\nInflation data saved to: {INFLATION_FILE}")

    # Log summary
    logger.info("\nYearly CPI Change (%):")
    for year in sorted(cpi_yearly.keys()):
        if cpi_yearly[year] is not None:
            logger.info(f"  {year}: {cpi_yearly[year]:.2f}%")

    logger.info("\nYearly Wage Change (%):")
    for year in sorted(wage_yearly.keys()):
        if wage_yearly[year] is not None:
            logger.info(f"  {year}: {wage_yearly[year]:.2f}%")


if __name__ == "__main__":
    main()
