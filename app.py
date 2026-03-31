#!/usr/bin/env python3
"""
Flask web application for Icelandic government budget comparison.
Displays and compares budget data across bills, plans, and accounts.
"""

from flask import Flask, render_template, request, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix
from pathlib import Path
import pandas as pd
import logging
import json

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Handle X-Script-Name header from reverse proxy (for /fjarlog prefix)
app.wsgi_app = ProxyFix(app.wsgi_app, x_script_name=1)

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
PLAN_COMPARISON_FILE = DATA_DIR / "plan_comparison.parquet"
INFLATION_FILE = PROJECT_DIR / "data" / "inflation_indices.json"

# Cache for loaded data
_comparison_df = None
_malefnasvid_df = None
_plan_df = None
_inflation_data = None


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


def load_plan_comparison_data():
    """Load plan comparison data from parquet file."""
    global _plan_df
    if _plan_df is None:
        if not PLAN_COMPARISON_FILE.exists():
            logger.error(f"Plan comparison file not found: {PLAN_COMPARISON_FILE}")
            return None
        _plan_df = pd.read_parquet(PLAN_COMPARISON_FILE)
    return _plan_df


def load_inflation_data():
    """Load inflation indices from JSON file."""
    global _inflation_data
    if _inflation_data is None:
        if not INFLATION_FILE.exists():
            logger.warning(f"Inflation file not found: {INFLATION_FILE}")
            return None
        try:
            with open(INFLATION_FILE, 'r', encoding='utf-8') as f:
                _inflation_data = json.load(f)
        except Exception as e:
            logger.error(f"Failed to load inflation data: {e}")
            return None
    return _inflation_data


def calculate_cumulative_inflation(from_year, to_year):
    """
    Calculate cumulative inflation rate from from_year to to_year.
    Uses weighted average of CPI (65%) and wage growth assumption (35%).
    Returns the adjustment factor (e.g., 1.25 means 25% inflation).
    """
    inflation_data = load_inflation_data()
    if not inflation_data:
        return 1.0

    cpi_yearly = inflation_data.get("cpi", {}).get("yearly", {})
    methodology = inflation_data.get("methodology", {}).get("cpi_wage_split", {})

    # Default weights if not in file
    cpi_weight = methodology.get("cpi", 0.65)
    wage_weight = methodology.get("wages", 0.35)

    # For simplicity, use CPI for both (wage data fetch hit rate limits)
    # In production, could use actual wage data or adjust weights
    cumulative_factor = 1.0

    for year in range(from_year, to_year):
        year_str = str(year)
        inflation_rate = cpi_yearly.get(year_str, 0) / 100.0

        # Apply weighted inflation
        weighted_inflation = inflation_rate * cpi_weight + inflation_rate * wage_weight
        cumulative_factor *= (1 + weighted_inflation)

    return cumulative_factor


def adjust_for_inflation(value, year_from, year_to):
    """Adjust a budget value for inflation from year_from to year_to."""
    if value is None or pd.isna(value):
        return value

    if year_from >= year_to:
        return value

    factor = calculate_cumulative_inflation(year_from, year_to)
    return value * factor


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
    """API endpoint for malefnasvið comparison data with filtering and inflation adjustment."""
    df = load_malefnasvid_comparison_data()
    if df is None:
        return jsonify({"error": "Data not available"}), 500

    # Get filter parameters
    year = request.args.get("year", type=int)
    source = request.args.get("source")  # 'plan', 'bill', or 'accounts'
    adjust_inflation_to = request.args.get("adjust_inflation_to", type=int)  # Target year for inflation adjustment

    # Apply year filter
    result = df.copy()
    if year:
        result = result[result["year"] == year]
        # For a specific year, return all areas with their amounts from each source
        result = result.sort_values("malefnasvid_nr")
        result_dict = result.to_dict("records")
        # Convert NaN to None for JSON serialization and apply inflation adjustment
        for row in result_dict:
            row_year = row.get("year")
            for key in ["amount_planned", "amount_billed", "amount_approved", "amount_actual"]:
                if pd.isna(row.get(key)):
                    row[key] = None
                elif adjust_inflation_to and row_year:
                    # Adjust value for inflation
                    row[key] = adjust_for_inflation(row[key], row_year, adjust_inflation_to)
                    # Add adjusted flag
                    row["inflation_adjusted"] = True
                    row["adjusted_to_year"] = adjust_inflation_to
        return jsonify(result_dict)

    # If source filter is applied, return cross-year data for that source
    if source:
        # Get the appropriate amount column
        source_column_map = {
            "plan": "amount_planned",
            "bill": "amount_billed",
            "bill_approved": "amount_approved",
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
                value = year_row[amount_col]
                year_val = year_row["year"]
                # Apply inflation adjustment if requested
                if adjust_inflation_to and year_val < adjust_inflation_to:
                    value = adjust_for_inflation(value, year_val, adjust_inflation_to)
                    if "inflation_adjusted" not in area_data:
                        area_data["inflation_adjusted"] = True
                        area_data["adjusted_to_year"] = adjust_inflation_to
                area_data[str(year_val)] = value
            pivot_data.append(area_data)

        return jsonify(pivot_data)

    # No filters - return all data
    result_dict = result.to_dict("records")
    for row in result_dict:
        for key in ["amount_planned", "amount_billed", "amount_approved", "amount_actual"]:
            if pd.isna(row.get(key)):
                row[key] = None
    return jsonify(result_dict)


@app.route("/api/plan")
def api_plan():
    """API endpoint for budget plan (fjármálaáætlun) data with inflation adjustment."""
    df = load_plan_comparison_data()
    if df is None:
        return jsonify({"error": "Plan data not available"}), 500

    # Get filter parameters
    year = request.args.get("year", type=int)
    malefnasvid_nr = request.args.get("malefnasvid_nr")
    adjust_inflation_to = request.args.get("adjust_inflation_to", type=int)

    # Apply filters
    result = df.copy()
    if year:
        result = result[result["year"] == year]
    if malefnasvid_nr:
        result = result[result["malefnasvid_nr"] == malefnasvid_nr]

    # Sort by year and plan_range for consistent display
    result = result.sort_values(["year", "malefnasvid_nr", "plan_range"])

    # Convert to JSON-serializable format
    result_dict = result.to_dict("records")
    for row in result_dict:
        amount = row.get("amount")
        document_id = row.get("document_id", "")

        if pd.isna(amount):
            row["amount"] = None
        elif adjust_inflation_to:
            # For plan data, extract the plan creation year from document_id (e.g., "plan_2022_2026" -> 2022)
            # This is important because a 2026 forecast made in 2022 should be adjusted FROM 2022 TO adjust_inflation_to
            plan_creation_year = None
            if document_id and document_id.startswith("plan_"):
                try:
                    parts = document_id.split("_")
                    if len(parts) >= 2:
                        plan_creation_year = int(parts[1])
                except (ValueError, IndexError):
                    pass

            # Use plan creation year if available, otherwise use fiscal year
            base_year = plan_creation_year if plan_creation_year else row.get("year")

            if base_year and base_year < adjust_inflation_to:
                # Adjust for inflation to target year
                row["amount"] = adjust_for_inflation(amount, base_year, adjust_inflation_to)
                row["inflation_adjusted"] = True
                row["adjusted_to_year"] = adjust_inflation_to

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
    years_with_bills_approved = sorted(df[df["amount_approved"].notna()]["year"].unique().tolist())
    years_with_accounts = sorted(df[df["amount_actual"].notna()]["year"].unique().tolist())

    return render_template(
        "comparison.html",
        years=years,
        years_with_plans=years_with_plans,
        years_with_bills=years_with_bills,
        years_with_bills_approved=years_with_bills_approved,
        years_with_accounts=years_with_accounts,
    )


@app.route("/malefnasvid/<area_code>")
def malefnasvid_detail(area_code):
    """Detail page for a specific malefnasvið showing institution breakdown."""
    # Load institution-level data
    institutions_file = PROJECT_DIR / "data" / "processed" / "budget_bills_approved" / f"bill_*_approved_institutions.parquet"

    # Try to find institution data files
    import glob
    institution_files = glob.glob(str(institutions_file))

    if not institution_files:
        return render_template("error.html", message="Institution data not available yet."), 404

    # Load all institution files and filter by area
    dfs = [pd.read_parquet(f) for f in institution_files]
    df_institutions = pd.concat(dfs, ignore_index=True) if dfs else None

    if df_institutions is None:
        return render_template("error.html", message="Institution data not available yet."), 404

    # Filter to requested area
    area_data = df_institutions[df_institutions["malefnasvid_nr"] == area_code]

    if area_data.empty:
        return render_template("error.html", message=f"No data found for málefnasvið {area_code}."), 404

    # Get area name from first row
    area_name = area_data.iloc[0]["malefnasvid"]

    # Get all unique areas for navigation buttons
    all_areas = df_institutions.drop_duplicates("malefnasvid_nr")[["malefnasvid_nr", "malefnasvid"]].sort_values("malefnasvid_nr")
    areas_list = [{"code": row["malefnasvid_nr"], "name": row["malefnasvid"]} for _, row in all_areas.iterrows()]

    return render_template(
        "malefnasvid_detail.html",
        area_code=area_code,
        area_name=area_name,
        all_areas=areas_list,
    )


@app.route("/api/malefnasvid/<area_code>/institutions")
def api_malefnasvid_institutions(area_code):
    """API endpoint for institution-level breakdown of a malefnasvið."""
    # Load institution-level data
    institutions_file = PROJECT_DIR / "data" / "processed" / "budget_bills_approved" / f"bill_*_approved_institutions.parquet"

    import glob
    institution_files = sorted(glob.glob(str(institutions_file)))

    if not institution_files:
        return jsonify({"error": "Institution data not available"}), 404

    # Load all institution files
    dfs = [pd.read_parquet(f) for f in institution_files]
    df_institutions = pd.concat(dfs, ignore_index=True) if dfs else None

    if df_institutions is None:
        return jsonify({"error": "Institution data not available"}), 404

    # Filter to requested area
    area_data = df_institutions[df_institutions["malefnasvid_nr"] == area_code]

    if area_data.empty:
        return jsonify({"error": f"No data found for málefnasvið {area_code}"}), 404

    # Get year filter if provided
    year = request.args.get("year", type=int)
    if year:
        area_data = area_data[area_data["year"] == year]

    # Convert to JSON, replacing all NaN with None
    result = area_data.to_dict("records")
    for row in result:
        for key in row:
            if pd.isna(row.get(key)):
                row[key] = None

    return jsonify(result)


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


@app.route("/fjarmalaaeatlun")
def fjarmalaaeatlun():
    """Budget plan (fjármálaáætlun) page."""
    df = load_plan_comparison_data()
    if df is None:
        return render_template("error.html", message="Plan data not available. Please run the pipeline first."), 500

    # Get years and policy areas that have data
    years = sorted(df["year"].unique().tolist())
    malefnasvid_nrs = sorted(df["malefnasvid_nr"].unique().tolist())
    plan_ranges = sorted(df["plan_range"].unique().tolist())

    return render_template(
        "fjarmalaaeatlun.html",
        years=years,
        malefnasvid_nrs=malefnasvid_nrs,
        plan_ranges=plan_ranges,
        current_year=2026
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
