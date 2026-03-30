#!/usr/bin/env python3
"""
Helper script to manually extract and validate institution-level data from approved budget addendums.

This script provides utilities to:
1. Generate templates for institution data entry
2. Validate institution data structure
3. Convert extracted data to the format needed for process_bills_approved_institutions.py

Usage: Populate the INSTITUTION_DATA below with values extracted from the PDF addendum,
then run this script to validate and format the data.
"""

import json
from pathlib import Path

# Template for complete 2020 institution data
# Based on the 35 málefnasvið structure from approved bills
# Each area should list its institutions with (institution_code, institution_name, heildarfjarhemild)

INSTITUTION_DATA_2020 = {
    # Areas 01-03: Already extracted and validated
    "01": ("Alþingi og eftirlitsstofnanir þess", 6892.9, [
        ("00-201", "Alþingi", 4193.5),
        ("00-205", "Framkvæmdir á Alþingisreit", 1600.0),
        ("00-610", "Umboðsmaður Alþingis", 293.8),
        ("00-620", "Ríkisendurskoðun", 805.6),
    ]),
    "02": ("Dómstólar", 3379.3, [
        ("00-401", "Hæstiréttur", 212.2),
        ("06-201", "Hæstiréttur", 244.3),
        ("06-210", "Héraðsdómstólar", 1831.0),
        ("06-205", "Landsréttur", 728.3),
        ("06-220", "Dómstólasýslan", 355.4),
        ("06-998", "Varasjóðir málaflokka", 8.1),
    ]),
    "03": ("Æðsta stjórnsýsla", 2904.7, [
        ("00-101", "Embætti forseta Íslands", 363.2),
        ("00-301", "Ríkisstjórn", 660.9),
        ("01-101", "Forsætisráðuneyti, aðalskrifstofa", 1880.6),
    ]),

    # Areas 04-35: Awaiting institution breakdown data
    # Template entry (replace with actual data):
    # "NN": ("Málefnasvið heiti", TOTAL_AMOUNT, [
    #     ("XX-XXX", "Institution name", amount),
    #     ...
    # ]),
}

def generate_template():
    """Generate a template for data entry."""
    print("=" * 80)
    print("TEMPLATE FOR INSTITUTION DATA ENTRY")
    print("=" * 80)
    print("""
For each málefsnasvið (04-35), provide:
  - Málefnasvið number and name
  - Total amount (heildarfjárheimild) for that área
  - List of institutions with codes and amounts

Format:
    "NN": ("Málefnasvið heiti", TOTAL, [
        ("XX-XXX", "Institution name", amount),
        ("XX-XXX", "Institution name", amount),
    ]),

Institution code format: XX-XXX (e.g., 01-101, 03-210, 10-100)
Amount format: decimal number (e.g., 1234.5 for 1,234.5 million kr)
    """)
    print("=" * 80)

def validate_data():
    """Validate that institution amounts sum correctly to málefnasvið totals."""
    print("\nVALIDATING DATA...")
    print("-" * 80)

    for area_code, (area_name, area_total, institutions) in INSTITUTION_DATA_2020.items():
        inst_sum = sum(amount for _, _, amount in institutions)
        diff = abs(inst_sum - area_total)

        status = "✓" if diff < 0.1 else "✗"
        print(f"{status} Area {area_code}: {area_name}")
        print(f"  Total: {area_total:.1f}, Institutions sum: {inst_sum:.1f}, Diff: {diff:.1f}")

        if institutions:
            for code, name, amount in institutions[:3]:  # Show first 3
                print(f"    - {code}: {name} ({amount})")
            if len(institutions) > 3:
                print(f"    ... and {len(institutions) - 3} more")

    print("-" * 80)

def export_to_python():
    """Export data in the format needed for process_bills_approved_institutions.py"""
    print("\n" + "=" * 80)
    print("PYTHON CODE FOR process_bills_approved_institutions.py")
    print("=" * 80)

    for area_code in sorted(INSTITUTION_DATA_2020.keys()):
        area_name, area_total, institutions = INSTITUTION_DATA_2020[area_code]
        print(f'\n        "{area_code}": {{')
        print(f'            "malefnasvid": "{area_name}",')
        print(f'            "institutions": {{')

        # Group institutions by their level (e.g., 01.10, 01.20, etc.)
        by_level = {}
        for code, name, amount in institutions:
            level = f"{area_code}.{code.split('-')[0]}"  # Simplified grouping
            if level not in by_level:
                by_level[level] = []
            by_level[level].append((code, name, amount))

        for level, insts in sorted(by_level.items()):
            level_name = insts[0][1]  # Use first institution's name as level name
            level_total = sum(amt for _, _, amt in insts)

            print(f'                "{level}": {{')
            print(f'                    "name": "{level_name}",')
            print(f'                    "heildarfjarhemild": {level_total},')
            print(f'                    "sub_institutions": {{')

            for code, name, amount in insts:
                print(f'                        "{code}": ("{name}", {amount}),')

            print(f'                    }}')
            print(f'                }},')

        print(f'            }}')
        print(f'        }},')

if __name__ == "__main__":
    print("Institution Data Extraction Helper")
    print("=" * 80)

    generate_template()
    validate_data()
    export_to_python()

    print("\n" + "=" * 80)
    print("To use this script:")
    print("1. Populate INSTITUTION_DATA_2020 with data from the 2020 addendum PDF")
    print("2. Run validate_data() to check sums")
    print("3. Use export_to_python() output to update process_bills_approved_institutions.py")
    print("=" * 80)
