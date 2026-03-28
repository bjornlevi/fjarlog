# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Fjárlagagreining is a Flask web application that aggregates and compares Icelandic government budget data from three official sources:

- **Frumvarp til fjárlaga** (Budget Bills) - Annual budget proposals
- **Tillaga til fjármálaáætlunar** (Budget Plans) - Multi-year financial estimates
- **Ríkisreikningur** (Budget Accounts) - Annual budget accounts/statements

The application displays comparative budget data across years, institutions, and budget line items via an interactive web interface.

## Architecture: Medallion Data Pipeline

The project implements a medallion (lakehouse) data architecture with three layers:

### Layer 1: Landing (Bronze)
- **Location**: `data/landing/`
- **Purpose**: Raw downloaded documents (CSV, XLSX, PDF files)
- **Content**: Unmodified files from government sources
- **Subdirectories**:
  - `budget_bills/` - Bill data files by year
  - `budget_plans/` - Plan data files by year range
  - `budget_accounts/` - Account data files by year

### Layer 2: Processed (Silver)
- **Location**: `data/processed/`
- **Purpose**: Cleaned and extracted data in parquet format
- **Content**: Structured tables extracted from landing documents
- **Subdirectories**:
  - `budget_bills/` - Extracted bill amounts by (year, institution, budget_line)
  - `budget_plans/` - Extracted plan amounts by (year_range, institution, budget_line)
  - `budget_accounts/` - Extracted account amounts by (year, institution, budget_line)
- **Format**: Parquet files (one per source document)
- **Key columns**: `year`, `institution`, `budget_line`, `amount`, `document_id`, `source_type`

### Layer 3: Curated (Gold)
- **Location**: `data/curated/`
- **Purpose**: Analysis-ready datasets for the web application
- **Content**: `comparison.parquet` - Single denormalized table joining all three sources
- **Key columns**:
  - `year` - Budget year
  - `institution` - Government institution
  - `budget_line` - Budget category/account
  - `amount_planned` - Amount from multi-year plan
  - `amount_billed` - Amount from annual bill proposal
  - `amount_actual` - Actual amount from account statement
  - `plan_document`, `bill_document`, `account_document` - Source document IDs

## Key Commands

All commands are defined in the Makefile. Common workflows:

### Full Pipeline Execution
```bash
make all              # Complete pipeline: scrape → download → process → curate
make pipeline         # Same as 'make all'
```

### Data Acquisition
```bash
make scrape           # Run all data source scrapers (bills, plans, accounts)
make scrape-bills     # Scrape budget bill URLs from stjornarradid.is
make scrape-plans     # Scrape budget plan URLs from stjornarradid.is
make scrape-accounts  # Notes: Budget account URLs are not auto-scrapable; must be added to data_sources.json manually
make download         # Download all documents to landing zone
```

### Data Processing
```bash
make process          # Process all data (plans, bills, accounts)
make process-bills    # Extract from bills only
make process-plans    # Extract from plans only
make process-accounts # Extract from accounts only
make curate           # Build comparison.parquet from processed data
```

### Web Application
```bash
make web              # Start Flask dev server on http://localhost:5000
make install          # Install Python dependencies from requirements.txt
```

### Cleanup
```bash
make clean            # Remove all downloaded and processed data files (keeps source config)
```

## Data Flow & Key Scripts

### Scraping Phase
- **`scrape_fjarlog.py`** - Scrapes budget bill URLs from stjornarradid.is, updates `data_sources.json`
- **`scrape_fjarmalaaaetlun.py`** - Scrapes budget plan URLs
- **`scrape_rikisreikningur.py`** - Budget accounts require manual URL entry (dynamically generated URLs)
- **`scrape_all_sources.py`** - Orchestrates all scrapers

**Note**: Budget account (Ríkisreikningur) data must be manually downloaded to `data/landing/budget_accounts/` since URLs are dynamically generated on the government website.

### Extraction Phase (Landing → Silver)
- **`process_bills.py`** - Parses CSV/XLSX bill files, extracts structured data
- **`process_plans.py`** - Parses XLSX plan files, handles multi-year year ranges
- **`process_accounts.py`** - Parses account files (PDF/XLSX)
- **`process_all_data.py`** - Orchestrates all processing steps with timeout handling

### Curation Phase (Silver → Gold)
- **`build_comparison.py`** - Joins processed data into a single comparison table:
  - Matches records by `(year, budget_line, institution)`
  - For plans: finds earliest plan covering the year (handles year ranges)
  - Creates one row per unique combination across all sources
  - Saves as `data/curated/comparison.parquet`

### Web Layer
- **`app.py`** - Flask application with routes:
  - `/` - Home page with data statistics
  - `/comparison` - Interactive table page for budget comparisons
  - `/budget-lines` - Budget line browser
  - `/api/comparison` - REST endpoint for filtered comparison data
  - `/about` - About page
- Loads comparison data once on first request (cached in `_comparison_df`)

### Supporting Files
- **`data_sources.json`** - Configuration of all document URLs by source and year. Updated by scrapers.
- **`requirements.txt`** - Python dependencies (Flask, pandas, pdfplumber, etc.)

## Development Patterns

### Processing Scripts Structure
All `process_*.py` scripts follow this pattern:
1. Read landing zone parquet/CSV files
2. Extract relevant columns (year, institution, budget_line, amount)
3. Normalize data (lowercase columns, consistent formats)
4. Write to processed zone with consistent schema
5. Log progress and warnings for missing/malformed data

### Data Extraction Challenges
- **Bills (CSV/XLSX)**: Multiple column headers (year estimates in different row positions across files)
- **Plans (XLSX)**: Year ranges in document ID; need to match plan coverage to specific years
- **Accounts (PDF/XLSX)**: Varying table structures and formats across years

### File Organization
- Processed files are organized by type: `budget_bills/`, `budget_plans/`, `budget_accounts/`
- Each processed file is named after its source document ID (e.g., `bill_2026.parquet`)
- Comparison joins on `(year, institution, budget_line)` - these are the primary keys

## Testing and Debugging

### Common Issues
1. **"Data not available" on web page** - Pipeline hasn't been run; execute `make all` or `make process && make curate`
2. **Missing processed files** - A processing step failed; check the logs and run individual `process_*.py` scripts
3. **URL download failures** - Check `data_sources.json` URLs and network connectivity
4. **PDF extraction errors** - Some PDF formats may be unsupported; check logs in `process_accounts.py`

### Verifying Data Integrity
```bash
# Load and inspect comparison data
python3 -c "import pandas as pd; df = pd.read_parquet('data/curated/comparison.parquet'); print(df.info()); print(df.head())"

# Check for missing values
python3 -c "import pandas as pd; df = pd.read_parquet('data/curated/comparison.parquet'); print(df.isnull().sum())"
```

## Dependencies

Core Python packages:
- **Flask 2.3+** - Web framework
- **pandas 2.0+** - Data manipulation
- **pyarrow 14+** - Parquet I/O
- **pdfplumber 0.10+** - PDF text extraction
- **openpyxl 3.0+** - XLSX reading
- **beautifulsoup4 4.12+** - HTML scraping
- **requests 2.31+** - HTTP downloads
- **python-dotenv 1.0+** - Environment configuration

All dependencies defined in `requirements.txt`.

## Web App Routing and Data Flow

The Flask app expects `data/curated/comparison.parquet` to exist:
- **Load behavior**: Data is loaded once on first request and cached globally
- **Filtering**: Year, institution, and budget_line filters are applied via query parameters
- **Error handling**: Returns 500 with error message if comparison file doesn't exist

Routes serve both HTML pages and JSON API responses for interactive filtering on the frontend.
