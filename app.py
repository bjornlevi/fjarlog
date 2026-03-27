#!/usr/bin/env python3
"""
Flask web application for Icelandic government budget comparison.
Displays and compares budget data across bills, plans, and accounts.
"""

from flask import Flask, render_template, request, jsonify
from pathlib import Path
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Paths
PROJECT_DIR = Path(__file__).parent
DATA_DIR = PROJECT_DIR / "data" / "curated"
COMPARISON_FILE = DATA_DIR / "comparison.parquet"

# Cache for loaded data
_comparison_df = None


def load_comparison_data():
    """Load comparison data from parquet file."""
    global _comparison_df
    if _comparison_df is None:
        if not COMPARISON_FILE.exists():
            logger.error(f"Comparison file not found: {COMPARISON_FILE}")
            return None
        _comparison_df = pd.read_parquet(COMPARISON_FILE)
    return _comparison_df


@app.route("/")
def index():
    """Home page with budget overview."""
    df = load_comparison_data()
    if df is None:
        return render_template("error.html", message="Data not available. Please run the pipeline first."), 500

    # Get basic stats
    stats = {
        "total_rows": len(df),
        "years": sorted(df["year"].unique().tolist()),
        "institutions": len(df["institution"].unique()),
        "budget_lines": len(df["budget_line"].unique()),
    }

    return render_template("index.html", stats=stats)


@app.route("/api/comparison")
def api_comparison():
    """API endpoint for comparison data with filtering."""
    df = load_comparison_data()
    if df is None:
        return jsonify({"error": "Data not available"}), 500

    # Get filter parameters
    year = request.args.get("year", type=int)
    institution = request.args.get("institution")
    budget_line = request.args.get("budget_line")

    # Apply filters
    result = df.copy()
    if year:
        result = result[result["year"] == year]
    if institution:
        result = result[result["institution"].str.contains(institution, case=False, na=False)]
    if budget_line:
        result = result[result["budget_line"].str.contains(budget_line, case=False, na=False)]

    # Convert to JSON-serializable format
    result = result.fillna("N/A")
    return jsonify(result.to_dict("records"))


@app.route("/comparison")
def comparison():
    """Comparison page with interactive table."""
    df = load_comparison_data()
    if df is None:
        return render_template("error.html", message="Data not available. Please run the pipeline first."), 500

    years = sorted(df["year"].unique().tolist())
    institutions = sorted(df["institution"].unique().tolist())

    return render_template(
        "comparison.html",
        years=years,
        institutions=institutions,
    )


@app.route("/budget-lines")
def budget_lines():
    """Page for browsing budget line items."""
    df = load_comparison_data()
    if df is None:
        return render_template("error.html", message="Data not available. Please run the pipeline first."), 500

    years = sorted(df["year"].unique().tolist())

    # Get budget lines for selected year
    selected_year = request.args.get("year", years[0] if years else None, type=int)
    if selected_year:
        year_data = df[df["year"] == selected_year]
        budget_lines = year_data["budget_line"].unique().tolist()
    else:
        budget_lines = []

    return render_template(
        "budget_lines.html",
        years=years,
        selected_year=selected_year,
        budget_lines=budget_lines,
    )


@app.route("/about")
def about():
    """About page."""
    return render_template("about.html")


@app.errorhandler(404)
def not_found(e):
    """Handle 404 errors."""
    return render_template("error.html", message="Page not found"), 404


@app.errorhandler(500)
def server_error(e):
    """Handle 500 errors."""
    return render_template("error.html", message="Server error"), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
