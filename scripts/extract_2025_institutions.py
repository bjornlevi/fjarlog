#!/usr/bin/env python3
"""
Extract institution-level data from 2025 approved budget bill addendum.
Uses the same segmentation approach as 2020/2021/2022/2023/2024: find all XX-XXX codes and map to areas.
"""

import re
import json
from pathlib import Path
import subprocess

PROJECT_DIR = Path(__file__).parent.parent
PDF_FILE = PROJECT_DIR / "data/landing/budget_bills_approved/2025/addendum/bill_2025_approved_addendum.pdf"
LAYOUT_TEXT = Path("/tmp/addendum_2025_layout.txt")
OUTPUT_JSON = Path("/tmp/institutions_2025_complete.json")

def extract_layout_text():
    """Extract text from PDF using pdftotext with layout preservation."""
    print("Extracting text from 2025 PDF (pages 11-90)...")
    subprocess.run(
        ["pdftotext", "-layout", "-f", "11", "-l", "90", str(PDF_FILE), str(LAYOUT_TEXT)],
        check=True
    )

    with open(LAYOUT_TEXT, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Find Yfirlit 2 section
    start_idx = None

    for i, line in enumerate(lines):
        if "Yfirlit 2" in line or "Fjárveitingar eftir stofnunum" in line:
            start_idx = i
            break

    if start_idx is None:
        print("Warning: Could not find Yfirlit 2 section, using page 11 onwards")
        start_idx = 250

    text = ''.join(lines[start_idx:])
    return text




def find_area_headers(text):
    """Find all málefnasvið headers (01-35)."""
    area_pattern = r'^(\d{2}) '
    area_headers = []

    for match in re.finditer(area_pattern, text, re.MULTILINE):
        area_code = match.group(1)
        if 1 <= int(area_code) <= 35:
            line_start = match.start()
            line_end = text.find('\n', line_start)
            full_line = text[line_start:line_end]
            name_match = re.match(r'\d{2}\s+([^.]+)', full_line)
            area_name = name_match.group(1).strip() if name_match else ""

            area_headers.append({
                'code': area_code,
                'name': area_name,
                'pos': line_start
            })

    # Remove duplicates (keep first)
    seen = set()
    unique = []
    for h in area_headers:
        if h['code'] not in seen:
            unique.append(h)
            seen.add(h['code'])

    return unique


def find_and_extract_institutions(text, area_headers):
    """Extract institutions by line-by-line parsing with area context tracking.

    Handles multi-line institutions by accumulating lines until we have 7 number groups.
    Properly assigns institutions to areas based on area header detection.
    """
    institutions_by_area = {}

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
            # New area header
            if current_block and len(re.findall(r'[\d.,]+', ' '.join(current_block))) >= 7:
                if current_area:
                    _save_institution(current_block, current_area, institutions_by_area)
                current_block = []

            area_code = area_match.group(1)
            area_name = area_match.group(2)
            current_area = {'code': area_code, 'name': area_name}
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

    return institutions_by_area


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

    # The last 7 numbers are the 7 budget columns (we only want the last one)
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


def build_institution_data(area_headers, institutions_by_area):
    """Build complete institution data dictionary."""
    institution_data = {}

    for area in area_headers:
        area_code = area['code']
        area_name = area['name']

        institutions = institutions_by_area.get(area_code, [])
        area_total = sum(i['heildarfjarhemild'] for i in institutions)

        institution_data[area_code] = (area_name, area_total, institutions)

    return institution_data


def main():
    print("=" * 80)
    print("2025 Institution-Level Extraction")
    print("=" * 80)

    # Extract text
    text = extract_layout_text()
    print(f"Extracted {len(text):,} characters\n")

    # Find area headers
    area_headers = find_area_headers(text)
    print(f"Found {len(area_headers)} area headers (01-35)")

    # Find and extract institutions
    print("Extracting all institutions by searching for XX-XXX pattern...")
    institutions_by_area = find_and_extract_institutions(text, area_headers)

    # Build complete data structure
    institution_data = build_institution_data(area_headers, institutions_by_area)

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
    code_file = Path("/tmp/institution_data_2025_code.py")
    code_lines = ['INSTITUTION_DATA_2025_COMPLETE = {']

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
