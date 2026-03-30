# Institution-Level Budget Data Extraction Status

## Overview

Extracted institution-level budget allocations from the 2020 approved budget bill addendum (Fylgirit með fjárlögum 2020), pages 11-61, section "Yfirlit 2: Fjárveitingar eftir stofnunum og verkefnum".

## Current Progress

### ✅ Completed
- **Areas 01-04**: Full institution-level breakdown extracted
  - Area 01 (Alþingi og eftirlitsstofnanir þess): 2 sub-areas, 4 institutions
  - Area 02 (Dómstólar): 4 sub-areas, 6 institutions
  - Area 03 (Æðsta stjórnsýsla): 3 sub-areas, 3 institutions
  - Area 04 (Utanríkismál): 2 sub-areas, 5 institutions
  - **Total**: 30 institution records across 11 sub-areas
  - **Total budget**: 18,950.0 ma.kr. (2.0% of 2020 total)

### API & UI Integration
- ✅ Web routes: `/malefnasvid/<area_code>` shows institutional drill-down
- ✅ API endpoint: `/api/malefnasvid/<area_code>/institutions?year=YYYY`
- ✅ Frontend table: Shows all institutions under each policy area with amounts
- ✅ Navigation: Clickable links from comparison table to institution detail pages

### ⏳ Remaining Work

**Areas 05-35** require institution-level data extraction. The 2020 addendum PDF contains this data but extraction requires:

1. **PDF Parsing Challenge**: The PDF uses complex table layouts with rotated text and non-standard formatting that standard PDF extraction tools struggle with
2. **81 Institution Records Found**: Raw extraction found 81 institution codes with amounts from the document, but mapping to the 35 policy areas requires careful cross-referencing

#### Extracted Institution Codes (Sample)
```
00-201 Alþingi
00-205 Framkvæmdir á Alþingisreit
00-301 Ríkisstjórn
00-610 Umboðsmaður Alþingis
03-300 Sendiráð Íslands
04-571 Orkustofnun
02-201 Háskóli Íslands
02-202 Tilraunastöð Háskólans að Keldum
14-231 Landgræðsla ríkissjóðs
... and 71 more
```

## Technical Details

### Data Model
Each institution record includes:
- `year`: Budget year (2020)
- `malefnasvid_nr`: Policy area number (01-35)
- `malefnasvid`: Policy area name (Icelandic)
- `institution_level`: Sub-area code (e.g., "01.10", "04.40")
- `institution_name`: Institution/sub-area name (Icelandic)
- `institution_code`: Institution code (e.g., "03-101", "04-571") or NULL for sub-area summaries
- `heildarfjarhemild`: Total budget authority (ma.kr.)
- `source_type`: "bill_approved_institutions"
- `document_id`: "bill_2020_approved_addendum"

### Files
- **Script**: `scripts/process_bills_approved_institutions.py`
  - Year-specific extractor for 2020
  - Extensible for future years (2021-2026)

- **Helper**: `scripts/extract_institutions_manual.py`
  - Template for data validation and conversion

- **Data Output**: `data/processed/budget_bills_approved/bill_2020_approved_institutions.parquet`
  - 30 records (areas 01-04)
  - Ready for API queries and web UI

## Next Steps

### To Complete Remaining Areas (05-35)

**Option 1: Manual Data Entry** (Fastest)
1. Use `scripts/extract_institutions_manual.py` as a template
2. Manually enter institution codes and amounts for areas 05-35
3. Validate totals against known area budgets
4. Update `process_bills_approved_institutions.py`
5. Re-run extraction: `python3 scripts/process_bills_approved_institutions.py`

**Option 2: Advanced PDF Processing** (Time-intensive)
1. Try alternative PDF libraries (e.g., `pdfminer.six`, `pypdf` with OCR)
2. Or: Download institution data from alternative government sources
3. Cross-reference with budget accounts data (which has institution codes)

**Option 3: Phased Approach**
- Provide high-impact areas first (e.g., 23-25: Health, 20-21: Education)
- Leave less-detailed areas (e.g., 26, 33: Financial/Administrative) for later

## User Impact

With the current data (areas 01-04), users can:
- Click on any of these 4 policy areas in the comparison table
- See institution-level budget breakdown
- Understand how budget is allocated across specific government institutions
- Drill down from high-level policy area summaries to individual institution amounts

For areas 05-35, the drill-down pages will show: *"Institution data not available yet"* until data is populated.

## Verification

Test institution drill-down:
```bash
# Start web app
make web

# Click on "01 Alþingi og eftirlitsstofnanir þess" in comparison table
# Should show all institutions (Alþingi, Framkvæmdir á Alþingisreit, etc.)

# Or API query:
curl http://localhost:5000/api/malefnasvid/04/institutions?year=2020
```

Expected response: JSON array with institutions for area 04 (Utanríkismál)

## Extracted Data Reference

Raw extracted data saved for future processing:
- `/tmp/all_institutions_2020.json` - 81 institutions with codes and amounts
- `/tmp/complete_institutions_2020.json` - Attempted complete mapping (partial)
- `/tmp/institutions_complete.txt` - Raw grep output from PDF text extraction

These can be used to complete the remaining areas once the area-to-institution mapping is finalized.
