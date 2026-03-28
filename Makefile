.PHONY: help install setup scrape scrape-bills scrape-plans scrape-accounts download process process-plans process-bills process-accounts curate clean all pipeline web

PYTHON := python3
PIP := pip3
SCRIPTS_DIR := scripts

help:
	@echo "Fjárlög - Budget Comparison Tool"
	@echo ""
	@echo "Available targets:"
	@echo ""
	@echo "Complete Pipeline:"
	@echo "  make all            Full pipeline: scrape → download → process → curate"
	@echo "  make pipeline       Same as 'make all'"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make install        Install dependencies"
	@echo "  make setup          Install dependencies"
	@echo ""
	@echo "Scraping:"
	@echo "  make scrape         Run all data source scrapers"
	@echo "  make scrape-bills   Scrape budget bills (frumvarp til fjárlaga)"
	@echo "  make scrape-plans   Scrape budget plans (Tillaga til fjármálaáætlunar)"
	@echo "  make scrape-accounts Scrape budget accounts (Ríkisreikningur)"
	@echo ""
	@echo "Data Pipeline Steps:"
	@echo "  make download       Download documents into landing zone"
	@echo "  make process        Extract and process all data files"
	@echo "  make process-plans     Process budget plans only"
	@echo "  make process-bills     Process budget bills only"
	@echo "  make process-accounts  Process budget accounts only"
	@echo "  make curate         Build final comparison parquet"
	@echo ""
	@echo "Web Application:"
	@echo "  make web            Start Flask web server (localhost:5000)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean          Remove all processed and downloaded data"
	@echo ""

install:
	$(PIP) install -r requirements.txt

setup: install

all: scrape download process curate
	@echo ""
	@echo "✓ Pipeline complete!"
	@echo "Comparison tables saved to:"
	@echo "  - data/curated/comparison.parquet"
	@echo "  - data/curated/malefnasvid_comparison.parquet"
	@echo ""

pipeline: all

scrape:
	@echo "Running all data source scrapers..."
	$(PYTHON) $(SCRIPTS_DIR)/scrape_all_sources.py

scrape-bills:
	@echo "Scraping budget bills (Frumvarp til fjárlaga)..."
	$(PYTHON) $(SCRIPTS_DIR)/scrape_fjarlog.py

scrape-plans:
	@echo "Scraping budget plans (Tillaga til fjármálaáætlunar)..."
	$(PYTHON) $(SCRIPTS_DIR)/scrape_fjarmalaaaetlun.py

scrape-accounts:
	@echo "Note: Budget account URLs are dynamically generated and cannot be automated."
	@echo "Please manually add account URLs to data_sources.json if needed."
	@echo "Skipping automated scraping for budget accounts."

download:
	@echo "Downloading documents to landing zone..."
	$(PYTHON) $(SCRIPTS_DIR)/download_sources.py

process:
	@echo "Processing all data (plans, bills, accounts)..."
	$(PYTHON) $(SCRIPTS_DIR)/process_all_data.py

process-plans:
	@echo "Processing budget plans..."
	$(PYTHON) $(SCRIPTS_DIR)/process_plans.py

process-bills:
	@echo "Processing budget bills..."
	$(PYTHON) $(SCRIPTS_DIR)/process_bills.py

process-accounts:
	@echo "Processing budget accounts..."
	$(PYTHON) $(SCRIPTS_DIR)/process_accounts.py

curate:
	@echo "Building comparison parquets..."
	$(PYTHON) $(SCRIPTS_DIR)/build_comparison.py
	$(PYTHON) $(SCRIPTS_DIR)/build_malefnasvid_comparison.py

web:
	@echo "Starting Flask web server..."
	@echo "Open http://localhost:5000 in your browser"
	$(PYTHON) app.py

clean:
	@echo "Cleaning up downloaded data files..."
	rm -rf data/landing/*
	rm -rf data/processed/*
	rm -rf data/curated/*
	@echo "Cleaned."
