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

# Custom Jinja2 filter for Icelandic number formatting
def format_number_is(value):
    """Format number with Icelandic thousands separator (.)"""
    if not isinstance(value, (int, float)):
        return value
    return "{:,}".format(int(value)).replace(",", ".")

app.jinja_env.filters['format_number_is'] = format_number_is

# Paths
PROJECT_DIR = Path(__file__).parent
DATA_DIR = PROJECT_DIR / "data" / "curated"
COMPARISON_FILE = DATA_DIR / "comparison.parquet"
MALEFNASVID_COMPARISON_FILE = DATA_DIR / "malefnasvid_comparison.parquet"

# Cache for loaded data
_comparison_df = None
_malefnasvid_df = None


def load_comparison_data():
    """Load comparison data from parquet file."""
    global _comparison_df
    if _comparison_df is None:
        if not COMPARISON_FILE.exists():
            logger.error(f"Comparison file not found: {COMPARISON_FILE}")
            return None
        _comparison_df = pd.read_parquet(COMPARISON_FILE)
    return _comparison_df


def load_malefnasvid_comparison_data():
    """Load malefnasvið comparison data from parquet file."""
    global _malefnasvid_df
    if _malefnasvid_df is None:
        if not MALEFNASVID_COMPARISON_FILE.exists():
            logger.error(f"Malefnasvið comparison file not found: {MALEFNASVID_COMPARISON_FILE}")
            return None
        _malefnasvid_df = pd.read_parquet(MALEFNASVID_COMPARISON_FILE)
    return _malefnasvid_df


@app.route("/")
def index():
    """Home page with budget overview."""
    df = load_comparison_data()
    if df is None:
        return render_template("error.html", message="Data not available. Please run the pipeline first."), 500

    # Filter out aggregate budget entries (not actual institutions)
    aggregate_categories = {
        'Annað', 'Frumgjöld', 'Frumjöfnuður', 'Frumtekjur', 'Government',
        'Heildargjöld', 'Heildarjöfnuður', 'Heildartekjur',
        'Vaxtagjöld', 'Vaxtajöfnuður', 'Vaxtatekjur', 'Vaxtagjöld ríkissjóðs'
    }

    df_institutions = df[~df["institution"].isin(aggregate_categories)]

    # Get basic stats
    stats = {
        "total_rows": len(df),
        "years": sorted(df["year"].unique().tolist()),
        "institutions": len(df_institutions["institution"].unique()),
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


@app.route("/api/malefnasvid")
def api_malefnasvid():
    """API endpoint for malefnasvið comparison data with filtering."""
    df = load_malefnasvid_comparison_data()
    if df is None:
        return jsonify({"error": "Data not available"}), 500

    # Get filter parameters
    year = request.args.get("year", type=int)
    source = request.args.get("source")  # 'plan', 'bill', or 'accounts'

    # Apply year filter
    result = df.copy()
    if year:
        result = result[result["year"] == year]
        # For a specific year, return all areas with their amounts from each source
        result = result.sort_values("malefnasvid_nr")
        result_dict = result.to_dict("records")
        # Convert NaN to None for JSON serialization
        for row in result_dict:
            for key in ["amount_planned", "amount_billed", "amount_actual"]:
                if pd.isna(row.get(key)):
                    row[key] = None
        return jsonify(result_dict)

    # If source filter is applied, return cross-year data for that source
    if source:
        # Get the appropriate amount column
        source_column_map = {
            "plan": "amount_planned",
            "bill": "amount_billed",
            "accounts": "amount_actual"
        }
        amount_col = source_column_map.get(source)

        if not amount_col:
            return jsonify({"error": f"Invalid source: {source}"}), 400

        # Filter to rows that have data for this source
        result = result[result[amount_col].notna()]
        result = result.sort_values(["malefnasvid_nr", "year"])

        # Pivot to get years as columns
        pivot_data = []
        for _, row in result.groupby("malefnasvid_nr"):
            area_data = {
                "malefnasvid_nr": row["malefnasvid_nr"].iloc[0],
                "malefnasvid": row["malefnasvid"].iloc[0],
            }
            for _, year_row in row.iterrows():
                area_data[str(year_row["year"])] = year_row[amount_col]
            pivot_data.append(area_data)

        return jsonify(pivot_data)

    # No filters - return all data
    result_dict = result.to_dict("records")
    for row in result_dict:
        for key in ["amount_planned", "amount_billed", "amount_actual"]:
            if pd.isna(row.get(key)):
                row[key] = None
    return jsonify(result_dict)


@app.route("/comparison")
def comparison():
    """Comparison page with malefnasvið side-by-side comparison."""
    df = load_malefnasvid_comparison_data()
    if df is None:
        return render_template("error.html", message="Data not available. Please run the pipeline first."), 500

    # Get years and sources that have data
    years = sorted(df["year"].unique().tolist())

    # Determine which sources have data for which years
    years_with_plans = sorted(df[df["amount_planned"].notna()]["year"].unique().tolist())
    years_with_bills = sorted(df[df["amount_billed"].notna()]["year"].unique().tolist())
    years_with_accounts = sorted(df[df["amount_actual"].notna()]["year"].unique().tolist())

    return render_template(
        "comparison.html",
        years=years,
        years_with_plans=years_with_plans,
        years_with_bills=years_with_bills,
        years_with_accounts=years_with_accounts,
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
