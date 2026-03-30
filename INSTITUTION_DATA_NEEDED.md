# Institution-Level Budget Data Extraction

## Current Status (2026-03-30)

The institution-level budget extraction system has been set up with:
- ✅ Script structure: `scripts/process_bills_approved_institutions.py`
- ✅ Data helper: `scripts/extract_institutions_manual.py`
- ✅ Parquet output: `data/processed/budget_bills_approved/bill_2020_approved_institutions.parquet`
- ⚠️ Partial data: Only areas 01-03 populated (22 records out of ~350+ expected)

## What's Needed: Complete 2020 Institution Breakdown

The 2020 approved bill addendum PDF contains a detailed institutional allocation table showing how each of the 35 policy areas (málefnasvið) is distributed across government institutions (with codes like `00-201`, `06-210`, etc.).

### Data Structure Required

For **each** of the 35 áreas (01-35), extract:

```
Area Code: NN
Area Name: "Policy Area Name"
Total Amount (Heildarfjárheimild): XXXX.X ma.kr.

Institutions:
  - Institution Code (XX-XXX)
    - Institution Name (Icelandic)
    - Allocation Amount (ma.kr.)

Example (Area 01):
  01-10: Alþingi
    - 00-201: Alþingi (4,193.5 ma.kr.)
    - 00-205: Framkvæmdir á Alþingisreit (1,600.0 ma.kr.)
  01-20: Eftirlitsstofnanir Alþingis
    - 00-610: Umboðsmaður Alþingis (293.8 ma.kr.)
    - 00-620: Ríkisendurskoðun (805.6 ma.kr.)
```

## 2020 Policy Areas with Budget Totals

All 35 áreas and their total approved budgets (source: `bill_2020_approved_malefnasvid.parquet`):

| Area | Name | Total (ma.kr.) |
|------|------|---|
| 01 | Alþingi og eftirlitsstofnanir þess | 6,892.9 |
| 02 | Dómstólar | 3,379.3 |
| 03 | Æðsta stjórnsýsla | 2,904.7 |
| 04 | Utanríkismál | 12,585.2 |
| 05 | Skatta-, eigna- og fjármálaumsýsla | 31,859.0 |
| 06 | Hagskýrslugerð og grunnskrár | 4,122.0 |
| 07 | Nýsköpun, rannsóknir og þekkingargreinar | 15,926.9 |
| 08 | Sveitarfélög og byggðamál | 23,419.6 |
| 09 | Almanna- og réttaröryggi | 30,520.1 |
| 10 | Rétt. einstakl., trúmál og stjórnsýsla dómsmála | 16,809.4 |
| 11 | Samgöngu- og fjarskiptamál | 47,736.7 |
| 12 | Landbúnaður | 16,605.7 |
| 13 | Sjávarútvegur og fiskeldi | 7,278.5 |
| 14 | Ferðaþjónusta | 2,005.0 |
| 15 | Orkumál | 4,521.5 |
| 16 | Markaðseftirlit, neytendamál og stj.sýsla atv.mála | 4,813.0 |
| 17 | Umhverfismál | 20,586.2 |
| 18 | Menning, listir, íþrótta- og æskulýðsmál | 16,158.4 |
| 19 | Fjölmiðlun | 5,302.1 |
| 20 | Framhaldsskólastig | 36,303.4 |
| 21 | Háskólastig | 45,275.2 |
| 22 | Önnur skólastig og stjórnsýsla mennta- og menn.mála | 5,549.8 |
| 23 | Sjúkrahúsþjónusta | 106,061.1 |
| 24 | Heilbrigðisþjónusta utan sjúkrahúsa | 56,736.2 |
| 25 | Hjúkrunar- og endurhæfingarþjónusta | 56,673.3 |
| 26 | Lyf og lækningavörur | 27,020.4 |
| 27 | Örorka og málefni fatlaðs fólks | 74,698.5 |
| 28 | Málefni aldraðra | 85,309.2 |
| 29 | Fjölskyldumál | 42,220.6 |
| 30 | Vinnumarkaður og atvinnuleysi | 36,516.2 |
| 31 | Húsnæðisstuðningur | 13,497.0 |
| 32 | Lýðheilsa og stjórnsýsla velferðarmála | 10,992.1 |
| 33 | Fjármagnskostnaður, ábyrgðir og lífeyrisskuldbindingar | 99,675.7 |
| 34 | Almennur varasjóður og sértækar fjárráðstafanir | 28,315.2 |
| 35 | Alþjóðleg þróunarsamvinna | 5,917.5 |

## How to Provide the Data

1. **Option A - Direct Input**: Extract from PDF addendum (pages ~115-140, showing "Yfirlit 2" table), then populate `scripts/extract_institutions_manual.py` with the data

2. **Option B - CSV/JSON**: Provide a structured file with institution breakdowns that can be imported

3. **Option C - Gradual**: Provide data for a few áreas at a time to populate iteratively

## Validation

Once complete data is provided, run:
```bash
python3 scripts/extract_institutions_manual.py
```

This will:
- Validate that institution amounts sum to área totals
- Generate Python code for `process_bills_approved_institutions.py`
- Show any discrepancies

## Integration Points

The institution-level data will enable:
1. ✅ Backend API: `/api/malefnasvid/:area/institutions` (return institution-level allocations)
2. ✅ Frontend: Drill-down view from policy area → constituent institutions
3. ✅ Comparison: See how institution budgets change year-to-year (when 2021-2026 data is also available)

## Next Steps

1. Extract institution breakdown data for 2020 from approved bill addendum
2. Populate remaining áreas (04-35) in the data structure
3. Run validation to ensure all totals match
4. Test the updated script: `python3 scripts/process_bills_approved_institutions.py`
5. (Future) Provide data for 2021-2026 addendums to extend institutional tracking
