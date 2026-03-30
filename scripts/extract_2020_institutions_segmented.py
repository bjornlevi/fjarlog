#!/usr/bin/env python3
"""
Extract institution-level data from 2020 approved budget bill addendum.
Uses improved segmentation: find each área (01-35) and capture all text until next área.
Works with pdftotext layout-formatted text.
"""

import re
import json
from pathlib import Path
import subprocess

PROJECT_DIR = Path(__file__).parent.parent
PDF_FILE = PROJECT_DIR / "data/landing/budget_bills_approved/2020/addendum/bill_2020_approved_addendum.pdf"
LAYOUT_TEXT = Path("/tmp/addendum_layout.txt")
OUTPUT_JSON = Path("/tmp/institutions_2020_segmented.json")

def extract_layout_text():
    """Extract text from PDF using pdftotext with layout preservation."""
    print("Extracting text from PDF using pdftotext...")

    subprocess.run(
        ["pdftotext", "-layout", str(PDF_FILE), str(LAYOUT_TEXT)],
        check=True
    )

    with open(LAYOUT_TEXT, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Find the Yfirlit 2 section
    start_idx = None
    for i, line in enumerate(lines):
        if "Yfirlit 2" in line:
            start_idx = i
            break

    if start_idx is None:
        raise ValueError("Could not find 'Yfirlit 2' in extracted text")

    return ''.join(lines[start_idx:])


def find_area_boundaries(text):
    """Find all área boundaries (01-35) with their start/end positions."""
    # Pattern: line starts with exactly XX (2 digits) + space, then area name
    area_pattern = r'^(\d{2}) '

    area_headers = []
    for match in re.finditer(area_pattern, text, re.MULTILINE):
        area_code = match.group(1)
        code_int = int(area_code)

        # Only consider areas 01-35
        if 1 <= code_int <= 35:
            # Extract the full line to get the area name
            line_start = match.start()
            line_end = text.find('\n', line_start)
            full_line = text[line_start:line_end]

            # Extract area name: everything between "XX " and the first series of dots
            # Split on dots and take the part before dots, then strip whitespace
            name_match = re.match(r'\d{2}\s+([^.]+)', full_line)
            area_name = name_match.group(1).strip() if name_match else ""

            area_headers.append({
                'code': area_code,
                'name': area_name,
                'start': line_start
            })

    # Remove duplicates (keep first occurrence of each area code)
    seen_codes = set()
    unique_headers = []
    for h in area_headers:
        if h['code'] not in seen_codes:
            unique_headers.append(h)
            seen_codes.add(h['code'])

    return unique_headers


def segment_by_areas(text, area_headers):
    """Create segments for each área from header to next header."""
    segments = {}

    for i, header in enumerate(area_headers):
        area_code = header['code']
        start_pos = header['start']

        # End position is the start of next area header
        if i + 1 < len(area_headers):
            end_pos = area_headers[i + 1]['start']
        else:
            end_pos = len(text)

        segment_text = text[start_pos:end_pos]
        segments[area_code] = {
            'name': header['name'],
            'text': segment_text
        }

    return segments


def extract_institutions(segment_text):
    """Extract institution codes, names, and heildarfjárheimild from a segment."""
    institutions = []

    for line in segment_text.split('\n'):
        # Check if line starts with 6 spaces and has XX-XXX pattern
        if not re.match(r'^      \d{2}-\d{3}\s+', line):
            continue

        match = re.match(r'^      (\d{2})-(\d{3})\s+(.+)$', line)
        if not match:
            continue

        ministry = match.group(1)
        code_suffix = match.group(2)
        rest_of_line = match.group(3)

        # Find where the amounts section starts (first digit)
        digit_pos = None
        for i, c in enumerate(rest_of_line):
            if c.isdigit():
                digit_pos = i
                break

        if digit_pos is None:
            continue

        # Everything before first digit is name + dots + spaces
        name_section = rest_of_line[:digit_pos].rstrip()
        amounts_section = rest_of_line[digit_pos:].strip()

        # Clean up name: remove trailing dots and spaces
        name = re.sub(r'\s*\.+\s*$', '', name_section).strip()

        if not name:
            continue

        # Extract all numeric values from the amounts section
        # They are comma-separated (comma=decimal, dot=thousands)
        amount_pattern = r'[\d.,]+'
        amounts = re.findall(amount_pattern, amounts_section)

        if len(amounts) >= 5:
            # 5th value is heildarfjárheimild (0-indexed: index 4)
            heildarfjarhemild_str = amounts[4]
            # Parse: remove thousands sep (.), replace decimal (,) with .
            heildarfjarhemild_str = heildarfjarhemild_str.replace('.', '').replace(',', '.')
            try:
                heildarfjarhemild = float(heildarfjarhemild_str)
                code = f"{ministry}-{code_suffix}"

                institutions.append({
                    'code': code,
                    'name': name,
                    'heildarfjarhemild': heildarfjarhemild
                })
            except ValueError:
                pass

    return institutions


def process_segments(segments):
    """Process all área segments to extract institution data."""
    institution_data = {}
    total_budget = 0
    total_institutions = 0

    for area_code in sorted(segments.keys()):
        info = segments[area_code]
        area_name = info['name']
        institutions = extract_institutions(info['text'])

        if institutions:
            area_total = sum(i['heildarfjarhemild'] for i in institutions)
            institution_data[area_code] = {
                'name': area_name,
                'total': area_total,
                'institutions': institutions
            }
            total_budget += area_total
            total_institutions += len(institutions)

            print(f"  {area_code} {area_name[:50]:50s} | {len(institutions):2d} institutions | {area_total:10.1f} ma.kr.")
        else:
            institution_data[area_code] = {
                'name': area_name,
                'total': 0.0,
                'institutions': []
            }
            print(f"  {area_code} {area_name[:50]:50s} | No institution breakdown")

    print(f"\nTotal: {total_institutions} institutions in {len([a for a in institution_data if institution_data[a]['institutions']])} areas")
    print(f"Total budget: {total_budget:.1f} ma.kr.")

    return institution_data


def generate_python_code(institution_data):
    """Generate Python code snippet for the institution data dictionary."""
    lines = ['INSTITUTION_DATA_2020 = {']

    for area_code in sorted(institution_data.keys()):
        info = institution_data[area_code]
        name_escaped = info['name'].replace('"', '\\"')

        lines.append(f'    "{area_code}": (')
        lines.append(f'        "{name_escaped}",')
        lines.append(f'        {info["total"]},')
        lines.append('        [')

        for inst in info['institutions']:
            inst_name_escaped = inst['name'].replace('"', '\\"')
            lines.append(f'            ("{inst["code"]}", "{inst_name_escaped}", {inst["heildarfjarhemild"]}),')

        lines.append('        ]')
        lines.append('    ),')

    lines.append('}')
    return '\n'.join(lines)


def main():
    print("=" * 80)
    print("2020 Approved Budget Bill - Institution-Level Extraction")
    print("=" * 80)

    # Step 1: Extract text
    text = extract_layout_text()
    print(f"Extracted {len(text):,} characters from Yfirlit 2 onwards\n")

    # Step 2: Find area boundaries
    area_headers = find_area_boundaries(text)
    print(f"Found {len(area_headers)} área boundaries (01-35)")

    # Step 3: Segment by areas
    segments = segment_by_areas(text, area_headers)
    print(f"Created {len(segments)} segments\n")

    # Step 4: Extract institutions
    print("Extracting institutions from each área:")
    institution_data = process_segments(segments)

    # Step 5: Save JSON
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(institution_data, f, ensure_ascii=False, indent=2)
    print(f"\nSaved JSON: {OUTPUT_JSON}")

    # Step 6: Generate Python code
    python_code = generate_python_code(institution_data)

    python_code_file = Path("/tmp/institution_data_2020_code.py")
    with open(python_code_file, 'w', encoding='utf-8') as f:
        f.write(python_code)
    print(f"Saved Python code: {python_code_file}\n")

    # Print code preview
    print("=" * 80)
    print("Generated Python Code (first 100 lines):")
    print("=" * 80)
    code_lines = python_code.split('\n')
    for line in code_lines[:100]:
        print(line)
    if len(code_lines) > 100:
        print(f"... ({len(code_lines) - 100} more lines)")


if __name__ == "__main__":
    main()
