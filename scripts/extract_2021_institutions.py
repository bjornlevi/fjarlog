#!/usr/bin/env python3
"""
Extract institution-level data from 2021 approved budget bill addendum.
Uses the same segmentation approach as 2020/2021/2022/2023: find all XX-XXX codes and map to areas.
"""

import re
import json
from pathlib import Path
import subprocess

PROJECT_DIR = Path(__file__).parent.parent
PDF_FILE = PROJECT_DIR / "data/landing/budget_bills_approved/2021/addendum/bill_2021_approved_addendum.pdf"
LAYOUT_TEXT = Path("/tmp/addendum_2021_layout.txt")
OUTPUT_JSON = Path("/tmp/institutions_2021_complete.json")

def extract_layout_text():
    """Extract text from PDF using pdftotext with layout preservation."""
    print("Extracting text from 2021 PDF (pages 11-50)...")
    subprocess.run(
        ["pdftotext", "-layout", str(PDF_FILE), str(LAYOUT_TEXT)],
        check=True
    )

    with open(LAYOUT_TEXT, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Find Yfirlit 2 or similar section
    start_idx = None
    for i, line in enumerate(lines):
        if "Yfirlit 2" in line or ("Fjárveitingar eftir stofnunum" in line):
            start_idx = i
            break

    if start_idx is None:
        print("Warning: Could not find Yfirlit 2 section, using page 11 onwards")
        # Page 11 is approximately line 250-300
        start_idx = 250

    return ''.join(lines[start_idx:])


def find_area_headers(text):
    """Extract area headers - not actively used but kept for compatibility."""
    return []


def find_and_extract_institutions(text, area_headers):
    """Extract institutions by line-by-line parsing with area context tracking.

    Handles multi-line institutions by accumulating lines until we have 7 number groups.
    Properly assigns institutions to areas based on area header detection.
    Returns: (institutions_by_area, area_names) where area_names maps code -> name
    """
    institutions_by_area = {}
    area_names = {}
    seen_area_codes = set()  # Track which area codes we've already seen

    lines = text.split('\n')
    current_block = []
    current_area = None

    for line in lines:
        # Clean line: collapse spaces/dots to single space
        cleaned = re.sub(r'[\s.]+', ' ', line).strip()

        if not cleaned:
            # Empty line - finalize block if we have one
            if current_block and len(re.findall(r'[\d.,]+', ' '.join(current_block))) >= 7:
                if current_area:
                    _save_institution(current_block, current_area, institutions_by_area)
                current_block = []
            continue

        # Check if this is an area header line (01-35 followed by space and name)
        # Capture area name up to the first digit
        area_match = re.match(r'^(0[1-9]|[12][0-9]|3[0-5])(?!\.\d)\s+([^0-9]+)', cleaned)
        if area_match:
            area_code = area_match.group(1)

            # Only accept FIRST occurrence of each area code (skip later duplicates)
            if area_code not in seen_area_codes:
                # New area header
                if current_block and len(re.findall(r'[\d.,]+', ' '.join(current_block))) >= 7:
                    if current_area:
                        _save_institution(current_block, current_area, institutions_by_area)
                    current_block = []

                area_name = area_match.group(2).strip()
                current_area = {'code': area_code, 'name': area_name}
                area_names[area_code] = area_name
                seen_area_codes.add(area_code)

            # Skip this header (it's a duplicate)
            continue

        if not current_area:
            continue

        # Check if line starts with institution code
        if re.match(r'^\d{2}-\d{3}', cleaned):
            # Start of new institution
            if current_block and len(re.findall(r'[\d.,]+', ' '.join(current_block))) >= 7:
                _save_institution(current_block, current_area, institutions_by_area)
            current_block = [cleaned]
        elif current_block:
            # Continuation of current institution
            current_block.append(cleaned)

            # Check if we have enough numbers to finalize
            if len(re.findall(r'[\d.,]+', ' '.join(current_block))) >= 7:
                _save_institution(current_block, current_area, institutions_by_area)
                current_block = []

    # Finalize last block
    if current_block and len(re.findall(r'[\d.,]+', ' '.join(current_block))) >= 7:
        if current_area:
            _save_institution(current_block, current_area, institutions_by_area)

    return institutions_by_area, area_names


def _save_institution(block, area, institutions_by_area):
    """Save a single institution from a block of lines."""
    if not block or not area:
        return

    full_text = ' '.join(block)

    # Extract code from first line
    code_match = re.match(r'^(\d{2})-(\d{3})', block[0])
    if not code_match:
        return

    code = f"{code_match.group(1)}-{code_match.group(2)}"

    # Extract name (everything in first line after code until first digit)
    first_line = block[0]
    code_end = first_line.find(code) + len(code)
    after_code = first_line[code_end:].strip()

    digit_pos = None
    for i, c in enumerate(after_code):
        if c.isdigit():
            digit_pos = i
            break

    if digit_pos is None:
        return

    name = after_code[:digit_pos].strip()
    if not name:
        return

    # Extract amounts - the last one is heildarfjárheimild (total budget authority)
    amounts = re.findall(r'[\d.,]+', full_text)
    if len(amounts) < 7:
        return

    # The last number is heildarfjárheimild
    heildarfjarhemild_str = amounts[-1]
    heildarfjarhemild_str = heildarfjarhemild_str.replace('.', '').replace(',', '.')

    try:
        heildarfjarhemild = float(heildarfjarhemild_str)
    except ValueError:
        return

    # Assign to area
    area_code = area['code']
    if area_code not in institutions_by_area:
        institutions_by_area[area_code] = []

    institutions_by_area[area_code].append({
        'code': code,
        'name': name,
        'heildarfjarhemild': heildarfjarhemild
    })


def build_institution_data(institutions_by_area, area_names):
    """Build complete institution data dictionary from extracted institutions."""
    institution_data = {}

    # Build from institutions_by_area which has all the area info
    for area_code in sorted(institutions_by_area.keys()):
        institutions = institutions_by_area[area_code]
        area_name = area_names.get(area_code, area_code)  # Use stored area name or code as fallback

        area_total = sum(i['heildarfjarhemild'] for i in institutions)
        institution_data[area_code] = (area_name, area_total, institutions)

    return institution_data


def main():
    print("=" * 80)
    print("2021 Institution-Level Extraction")
    print("=" * 80)

    # Extract text
    text = extract_layout_text()
    print(f"Extracted {len(text):,} characters\n")

    # Find area headers (not used directly, but kept for compatibility)
    area_headers = find_area_headers(text)
    print(f"Found {len(area_headers)} area headers (01-35)")

    # Find and extract institutions
    print("Extracting all institutions by searching for XX-XXX pattern...")
    institutions_by_area, area_names = find_and_extract_institutions(text, area_headers)

    # Build complete data structure
    institution_data = build_institution_data(institutions_by_area, area_names)

    # Print summary
    print("\nExtraction Summary by Area:")
    print("-" * 80)
    total_institutions = 0
    total_budget = 0

    for area_code in sorted(institution_data.keys()):
        area_name, area_total, institutions = institution_data[area_code]
        count = len(institutions)
        total_institutions += count
        total_budget += area_total

        print(f"  {area_code} {area_name[:55]:55s} | {count:4d} inst | {area_total:12.1f}")

    print("-" * 80)
    print(f"TOTAL: {total_institutions} institutions, {total_budget:,.1f} ma.kr.")
    print()

    # Save JSON
    json_data = {}
    for area_code, (area_name, area_total, institutions) in institution_data.items():
        json_data[area_code] = {
            'name': area_name,
            'total': area_total,
            'institutions': institutions
        }

    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"Saved JSON: {OUTPUT_JSON}\n")

    # Generate and save Python code
    code_file = Path("/tmp/institution_data_2021_code.py")
    code_lines = ['INSTITUTION_DATA_2021_COMPLETE = {']

    for area_code in sorted(institution_data.keys()):
        area_name, area_total, institutions = institution_data[area_code]
        area_name_escaped = area_name.replace('"', '\\"')

        code_lines.append(f'    "{area_code}": (')
        code_lines.append(f'        "{area_name_escaped}",')
        code_lines.append(f'        {area_total},')
        code_lines.append('        [')

        for inst in institutions:
            name_escaped = inst['name'].replace('"', '\\"')
            code_lines.append(f'            ("{inst["code"]}", "{name_escaped}", {inst["heildarfjarhemild"]}),')

        code_lines.append('        ]')
        code_lines.append('    ),')

    code_lines.append('}')
    python_code = '\n'.join(code_lines)

    with open(code_file, 'w', encoding='utf-8') as f:
        f.write(python_code)
    print(f"Saved Python code: {code_file}\n")

    # Print preview
    print("=" * 80)
    print("Python Code Preview (first 60 lines):")
    print("=" * 80)
    for line in code_lines[:60]:
        print(line)
    if len(code_lines) > 60:
        print(f"... ({len(code_lines) - 60} more lines)")


if __name__ == "__main__":
    main()
