# Fjárlög - Budget Comparison Tool

A Flask web application for displaying and comparing Icelandic government budget information from multiple sources.

## Data Sources

This project aggregates budget information from three document types:

- **Frumvarp til fjárlaga** (Budget Bills) - Annual budget proposals
- **Tillaga til fjármálaáætlunar** (Budget Plans) - Multi-year financial estimates
- **Ríkisreikningur** (Budget Accounts) - Annual budget accounts/statements

## Architecture

The project uses a medallion data pipeline:

- **Landing (Bronze)** - Raw downloaded documents
- **Processed (Silver)** - Cleaned and extracted data
- **Curated (Gold)** - Analysis-ready datasets

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Configure data sources
# Edit data_sources.json and add document URLs

# Download documents
python scripts/download_sources.py

# Run Flask app
python app.py
```

## Project Structure

```
fjarlog/
├── data_sources.json          # Configuration of document URLs
├── scripts/
│   └── download_sources.py    # Download documents from URLs
├── data/
│   ├── landing/               # Raw downloaded files
│   ├── processed/             # Transformed data
│   └── curated/               # Analysis-ready data
└── app.py                     # Flask application
```

## Related Projects

- [opin_gogn/rikid](../opin_gogn/rikid) - Government institution payment data
