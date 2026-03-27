# Flask Web Application Guide

## Starting the Web Server

To start the Flask development server:

```bash
make web
```

The app will be available at `http://localhost:5000`

To run without make:
```bash
python3 app.py
```

## Features

### Home Page (`/`)
- Overview of available data
- Statistics on number of records, years, institutions
- Links to data sources at Stjórnarráð Íslands

### Comparison Page (`/comparison`)
- Interactive table comparing budget plans vs bills
- Filter by:
  - **Year**: Select specific year
  - **Institution**: Search by ministry or government agency
  - **Budget Line**: Search by budget category or line item
- Shows three amount columns:
  - **Áætlað** (Planned): Amount from budget plan
  - **Gjaldreikning** (Billed): Amount from actual bill
  - **Raunverulegur** (Actual): Amount from government accounts (when available)
- Color coding:
  - Green: Plan and bill amounts match closely
  - Yellow: Mismatch between planned and actual
  - Red: Missing planned amount

### Budget Lines Page (`/budget-lines`)
- Browse all budget line items for a selected year
- Search/filter budget lines
- Quick link to comparison for each line

### About Page (`/about`)
- Project overview
- Data sources and licenses
- Technical architecture explanation
- Future planned improvements

## API Endpoints

### `GET /api/comparison`
Returns filtered comparison data as JSON.

**Parameters:**
- `year` (int, optional): Filter by year
- `institution` (string, optional): Filter by institution name (substring search)
- `budget_line` (string, optional): Filter by budget line name (substring search)

**Example:**
```bash
curl "http://localhost:5000/api/comparison?year=2025&institution=Forsæ"
```

**Response:**
```json
[
  {
    "year": 2025,
    "institution": "01 Forsætisráðuneyti",
    "budget_line": "Aðministrativ kostnaður",
    "amount_planned": 1234.5,
    "amount_billed": 1200.3,
    "amount_actual": null,
    "plan_document": "plan_2025_2029",
    "bill_document": "bill_2025",
    "account_document": null
  },
  ...
]
```

## Data Requirements

The app requires processed data from the pipeline:
- `/data/curated/comparison.parquet` - Main comparison table

Run the data pipeline first:
```bash
make all          # Full pipeline
make process      # Just process existing data
make curate       # Build comparison table
```

## Configuration

### Debug Mode
To run with debug mode enabled:
```bash
FLASK_ENV=development python3 app.py
```

### Production
For production deployment, use a WSGI server like Gunicorn:
```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 app:app
```

## Troubleshooting

### "Data not available" Error
The comparison.parquet file doesn't exist. Run the full pipeline:
```bash
make all
```

### Port Already in Use
Change the port in `app.py`:
```python
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5001)  # Use 5001 instead
```

### Module Not Found Errors
Ensure all dependencies are installed:
```bash
pip install -r requirements.txt
```

## Customization

### Adding New Pages
1. Create a new HTML template in `templates/`
2. Add a new route in `app.py`
3. Update navigation in `templates/base.html`

### Styling
Main CSS is in `templates/base.html` in the `<style>` block. Modify or extend as needed.

### Language
The app is in Icelandic by default. To change:
- Update text in HTML templates
- Update form labels and button text in JavaScript
