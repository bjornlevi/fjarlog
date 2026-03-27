.PHONY: help install setup scrape scrape-bills scrape-plans scrape-accounts download process process-plans process-bills process-accounts curate clean

PYTHON := python3
PIP := pip3
SCRIPTS_DIR := scripts

help:
	@echo "Fjárlög - Budget Comparison Tool"
	@echo ""
	@echo "Available targets:"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  make install        Install dependencies"
	@echo "  make setup          Install dependencies and Playwright browsers"
	@echo ""
	@echo "Scraping:"
	@echo "  make scrape         Run all data source scrapers"
	@echo "  make scrape-bills   Scrape budget bills (frumvarp til fjárlaga)"
	@echo "  make scrape-plans   Scrape budget plans (Tillaga til fjármálaáætlunar)"
	@echo "  make scrape-accounts Scrape budget accounts (Ríkisreikningur)"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make download       Download documents into landing zone"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean          Remove downloaded data files"
	@echo ""

install:
	$(PIP) install -r requirements.txt

setup: install
	$(PYTHON) -m playwright install

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
	@echo "Scraping budget accounts (Ríkisreikningur)..."
	$(PYTHON) $(SCRIPTS_DIR)/scrape_rikisreikningur.py

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
	@echo "Building comparison parquet..."
	$(PYTHON) $(SCRIPTS_DIR)/build_comparison.py

clean:
	@echo "Cleaning up downloaded data files..."
	rm -rf data/landing/*
	rm -rf data/processed/*
	rm -rf data/curated/*
	@echo "Cleaned."
