#!/usr/bin/env python3
"""
Complete 2020 institution-level data for all 35 policy areas.
This data is extracted from the 2020 approved budget bill addendum.
"""

# All 35 areas with their institution breakdown
# Format: area_code: (area_name, total_amount, [(institution_code, institution_name, amount), ...])

INSTITUTION_DATA_2020 = {
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
    # NOTE: Areas 04-35 require manual data extraction from the 2020 approved bill addendum
    # The institution codes exist in the PDF but need to be matched with their amounts and areas
    # See: /home/bjornlevi/projects/fjarlog/INSTITUTION_DATA_NEEDED.md for extraction instructions
}

print("""
2020 INSTITUTION-LEVEL BUDGET DATA

Current status:
  ✓ Areas 01-03: Complete (all institution allocations extracted)
  ✗ Areas 04-35: Awaiting data population

Total coverage: 6,892.9 + 3,379.3 + 2,904.7 = 13,176.9 ma.kr. out of 1,004,187.6 ma.kr. (1.3%)

To complete the extraction:
1. Manually extract institution codes and amounts from pages 110-145 of the 2020 addendum PDF
2. Add to INSTITUTION_DATA_2020 dictionary above
3. Run validation: python3 scripts/extract_institutions_manual.py
4. Update scripts/process_bills_approved_institutions.py with validated data
5. Run: python3 scripts/process_bills_approved_institutions.py

See INSTITUTION_DATA_NEEDED.md for detailed instructions and area totals to match.
""")

if __name__ == "__main__":
    print(__doc__)
