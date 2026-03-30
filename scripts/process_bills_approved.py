#!/usr/bin/env python3
"""
Extract approved budget bill data from PDF files into parquet format.
Extracts "Framlag úr ríkissjóði" (grant from treasury) amounts per policy area.
Part of the silver layer processing.

Each year may have different PDF structure, so year-specific extractors are maintained.
"""

import logging
from pathlib import Path
from typing import Optional, Dict, List
import pandas as pd
import pdfplumber
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
LANDING_DIR = PROJECT_DIR / "data" / "landing" / "budget_bills_approved"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed" / "budget_bills_approved"

# Create output directory
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def find_pdf_files() -> List[Path]:
    """Find all PDF files in the approved budget bills landing directory."""
    return sorted(LANDING_DIR.glob("*/*.pdf"))


def extract_2020_approved(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract 2020 approved budget bill data from PDF.

    2020 has rotated table on pages 5-6 with "Heildarfjárheimild" column.
    Manual values extracted from pages 5-6 (5th column in the table).
    """
    logger.info(f"  Using 2020-specific extractor for {file_path.name}")

    # Manually extracted "Heildarfjárheimild" values from pages 5-6
    # These are the definitive values from the approved budget bill
    heildarfjarhemild_2020 = {
        1: (6892.9, "Alþingi og eftirlitsstofnanir þess"),
        2: (3379.3, "Dómstólar"),
        3: (2904.7, "Æðsta stjórnsýsla"),
        4: (12585.2, "Utanríkismál"),
        5: (31859.0, "Skatta-, eigna- og fjármálaumsýsla"),
        6: (4122.0, "Hagskýrslugerð og grunnskrár"),
        7: (15926.9, "Nýsköpun, rannsóknir og þekkingargreinar"),
        8: (23419.6, "Sveitarfélög og byggðamál"),
        9: (30520.1, "Almanna- og réttaröryggi"),
        10: (16809.4, "Rétt. einstakl., trúmál og stjórnsýsla dómsmála"),
        11: (47736.7, "Samgöngu- og fjarskiptamál"),
        12: (16605.7, "Landbúnaður"),
        13: (7278.5, "Sjávarútvegur og fiskeldi"),
        14: (2005.0, "Ferðaþjónusta"),
        15: (4521.5, "Orkumál"),
        16: (4813.0, "Markaðseftirlit, neytendamál og stj.sýsla atv.mála"),
        17: (20586.2, "Umhverfismál"),
        18: (16158.4, "Menning, listir, íþrótta- og æskulýðsmál"),
        19: (5302.1, "Fjölmiðlun"),
        20: (36303.4, "Framhaldsskólastig"),
        21: (45275.2, "Háskólastig"),
        22: (5549.8, "Önnur skólastig og stjórnsýsla mennta- og menn.mála"),
        23: (106061.1, "Sjúkrahúsþjónusta"),
        24: (56736.2, "Heilbrigðisþjónusta utan sjúkrahúsa"),
        25: (56673.3, "Hjúkrunar- og endurhæfingarþjónusta"),
        26: (27020.4, "Lyf og lækningavörur"),
        27: (74698.5, "Örorka og málefni fatlaðs fólks"),
        28: (85309.2, "Málefni aldraðra"),
        29: (42220.6, "Fjölskyldumál"),
        30: (36516.2, "Vinnumarkaður og atvinnuleysi"),
        31: (13497.0, "Húsnæðisstuðningur"),
        32: (10992.1, "Lýðheilsa og stjórnsýsla velferðarmála"),
        33: (99675.7, "Fjármagnskostnaður, ábyrgðir og lífeyrisskuldbindingar"),
        34: (28315.2, "Almennur varasjóður og sértækar fjárráðstafanir"),
        35: (5917.5, "Alþjóðleg þróunarsamvinna"),
    }

    result_rows = []
    for area_num, (amount, area_name) in heildarfjarhemild_2020.items():
        result_rows.append({
            "year": 2020,
            "malefnasvid_nr": f"{area_num:02d}",
            "malefnasvid": area_name,
            "source_type": "bill_approved",
            "document_id": "bill_2020_approved",
            "amount": amount,
        })

    logger.info(f"  Extracted {len(result_rows)} records from 2020 approved bill")
    return pd.DataFrame(result_rows)


def extract_2021_approved(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract 2021 approved budget bill data from PDF.

    2021 has rotated table on pages 5-6 with "Heildarfjárheimild" column.
    Manual values extracted from pages 5-6 (5th column in the table).
    """
    logger.info(f"  Using 2021-specific extractor for {file_path.name}")

    # Manually extracted "Heildarfjárheimild" values from 2021 approved bill
    # These are the definitive values from pages 5-6 (5th column)
    heildarfjarhemild_2021 = {
        1: (6960.8, "Alþingi og eftirlitsstofnanir þess"),
        2: (3480.4, "Dómstólar"),
        3: (2708.3, "Æðsta stjórnsýsla"),
        4: (14100.5, "Utanríkismál"),
        5: (36745.3, "Skatta-, eigna- og fjármálaumsýsla"),
        6: (4125.2, "Hagskýrslugerð og grunnskrár"),
        7: (28788.1, "Nýsköpun, rannsóknir og þekkingargreinar"),
        8: (22473.4, "Sveitarfélög og byggðamál"),
        9: (31300.4, "Almanna- og réttaröryggi"),
        10: (17286.6, "Rétt. einstakl., trúmál og stjórnsýsla dómsmála"),
        11: (59503.1, "Samgöngu- og fjarskiptamál"),
        12: (18709.3, "Landbúnaður"),
        13: (9463.1, "Sjávarútvegur og fiskeldi"),
        14: (2574.1, "Ferðaþjónusta"),
        15: (5118.3, "Orkumál"),
        16: (4463.8, "Markaðseftirlit, neytendamál og stj.sýsla atv.mála"),
        17: (24351.6, "Umhverfismál"),
        18: (17608.5, "Menning, listir, íþrótta- og æskulýðsmál"),
        19: (5138.3, "Fjölmiðlun"),
        20: (40551.4, "Framhaldsskólastig"),
        21: (53167.6, "Háskólastig"),
        22: (5674.0, "Önnur skólastig og stjórnsýsla mennta- og menn.mála"),
        23: (121454.1, "Sjúkrahúsþjónusta"),
        24: (59816.7, "Heilbrigðisþjónusta utan sjúkrahúsa"),
        25: (64042.3, "Hjúkrunar- og endurhæfingarþjónusta"),
        26: (30439.7, "Lyf og lækningavörur"),
        27: (83609.3, "Örorka og málefni fatlaðs fólks"),
        28: (93419.9, "Málefni aldraðra"),
        29: (47118.1, "Fjölskyldumál"),
        30: (95472.6, "Vinnumarkaður og atvinnuleysi"),
        31: (13048.4, "Húsnæðisstuðningur"),
        32: (14923.2, "Lýðheilsa og stjórnsýsla velferðarmála"),
        33: (108317.8, "Fjármagnskostnaður, ábyrgðir og lífeyrisskuldbindingar"),
        34: (39354.7, "Almennur varasjóður og sértækar fjárráðstafanir"),
        35: (6789.8, "Alþjóðleg þróunarsamvinna"),
    }

    result_rows = []
    for area_num, (amount, area_name) in heildarfjarhemild_2021.items():
        result_rows.append({
            "year": 2021,
            "malefnasvid_nr": f"{area_num:02d}",
            "malefnasvid": area_name,
            "source_type": "bill_approved",
            "document_id": "bill_2021_approved",
            "amount": amount,
        })

    logger.info(f"  Extracted {len(result_rows)} records from 2021 approved bill")
    return pd.DataFrame(result_rows)


def extract_2022_approved(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract 2022 approved budget bill data from PDF.

    2022 has rotated table on pages 5-6 with "Heildarfjárheimild" column.
    Manual values extracted from pages 5-6 (5th column in the table).
    """
    logger.info(f"  Using 2022-specific extractor for {file_path.name}")

    # Manually extracted "Heildarfjárheimild" values from 2022 approved bill
    # These are the definitive values from pages 5-6 (5th column)
    heildarfjarhemild_2022 = {
        1: (7317.6, "Alþingi og eftirlitsstofnanir þess"),
        2: (3685.2, "Dómstólar"),
        3: (3603.9, "Æðsta stjórnsýsla"),
        4: (13961.9, "Utanríkismál"),
        5: (39423.7, "Skatta-, eigna- og fjármálaumsýsla"),
        6: (4182.9, "Hagskýrslugerð og grunnskrár"),
        7: (29108.0, "Nýsköpun, rannsóknir og þekkingargreinar"),
        8: (26946.7, "Sveitarfélög og byggðamál"),
        9: (34939.1, "Almanna- og réttaröryggi"),
        10: (17844.4, "Rétt. einstakl., trúmál og stjórnsýsla dómsmála"),
        11: (51444.6, "Samgöngu- og fjarskiptamál"),
        12: (19216.8, "Landbúnaður"),
        13: (8846.5, "Sjávarútvegur og fiskeldi"),
        14: (2279.2, "Ferðaþjónusta"),
        15: (6195.0, "Orkumál"),
        16: (4653.3, "Markaðseftirlit, neytendamál og stj.sýsla atv.mála"),
        17: (25354.5, "Umhverfismál"),
        18: (17897.0, "Menning, listir, íþrótta- og æskulýðsmál"),
        19: (5563.1, "Fjölmiðlun"),
        20: (40483.5, "Framhaldsskólastig"),
        21: (57129.4, "Háskólastig"),
        22: (6157.9, "Önnur skólastig og stjórnsýsla mennta- og menn.mála"),
        23: (137688.2, "Sjúkrahúsþjónusta"),
        24: (66896.5, "Heilbrigðisþjónusta utan sjúkrahúsa"),
        25: (71358.5, "Hjúkrunar- og endurhæfingarþjónusta"),
        26: (31279.6, "Lyf og lækningavörur"),
        27: (88935.0, "Örorka og málefni fatlaðs fólks"),
        28: (100985.3, "Málefni aldraðra"),
        29: (51778.0, "Fjölskyldumál"),
        30: (55101.1, "Vinnumarkaður og atvinnuleysi"),
        31: (13038.1, "Húsnæðisstuðningur"),
        32: (14838.6, "Lýðheilsa og stjórnsýsla velferðarmála"),
        33: (114191.2, "Fjármagnskostnaður, ábyrgðir og lífeyrisskuldbindingar"),
        34: (35346.9, "Almennur varasjóður og sértækar fjárráðstafanir"),
        35: (10247.3, "Alþjóðleg þróunarsamvinna"),
    }

    result_rows = []
    for area_num, (amount, area_name) in heildarfjarhemild_2022.items():
        result_rows.append({
            "year": 2022,
            "malefnasvid_nr": f"{area_num:02d}",
            "malefnasvid": area_name,
            "source_type": "bill_approved",
            "document_id": "bill_2022_approved",
            "amount": amount,
        })

    logger.info(f"  Extracted {len(result_rows)} records from 2022 approved bill")
    return pd.DataFrame(result_rows)


def extract_2023_approved(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract 2023 approved budget bill data from PDF.

    2023 has rotated table on pages 5-6 with "Heildarfjárheimild" column.
    Manual values extracted from pages 5-6 (5th column in the table).
    """
    logger.info(f"  Using 2023-specific extractor for {file_path.name}")

    # Manually extracted "Heildarfjárheimild" values from 2023 approved bill
    # These are the definitive values from pages 5-6 (5th column)
    heildarfjarhemild_2023 = {
        1: (7169.1, "Alþingi og eftirlitsstofnanir þess"),
        2: (3839.1, "Dómstólar"),
        3: (2855.4, "Æðsta stjórnsýsla"),
        4: (16786.2, "Utanríkismál"),
        5: (27256.3, "Skatta-, eigna- og fjármálaumsýsla"),
        6: (3323.3, "Hagskýrslugerð og grunnskrár"),
        7: (34892.2, "Nýsköpun, rannsóknir og þekkingargreinar"),
        8: (30369.1, "Sveitarfélög og byggðamál"),
        9: (38859.7, "Almanna- og réttaröryggi"),
        10: (20045.9, "Rétt. einstakl., trúmál og stjórnsýsla dómsmála"),
        11: (52417.8, "Samgöngu- og fjarskiptamál"),
        12: (21379.1, "Landbúnaður"),
        13: (6866.1, "Sjávarútvegur og fiskeldi"),
        14: (2376.7, "Ferðaþjónusta"),
        15: (8461.4, "Orkumál"),
        16: (3548.5, "Markaðseftirlit og neytendamál"),
        17: (29892.2, "Umhverfismál"),
        18: (20993.3, "Menning, listir, íþrótta- og æskulýðsmál"),
        19: (6281.8, "Fjölmiðlun"),
        20: (42140.9, "Framhaldsskólastig"),
        21: (60196.0, "Háskólastig"),
        22: (5493.1, "Önnur skólastig og stjórnsýsla mennta- og barnamála"),
        23: (143477.1, "Sjúkrahúsþjónusta"),
        24: (78541.2, "Heilbrigðisþjónusta utan sjúkrahúsa"),
        25: (77118.8, "Hjúkrunar- og endurhæfingarþjónusta"),
        26: (36514.2, "Lyf og lækningavörur"),
        27: (100539.2, "Örorka og málefni fatlaðs fólks"),
        28: (115017.3, "Málefni aldraðra"),
        29: (58314.3, "Fjölskyldumál"),
        30: (38460.1, "Vinnumarkaður og atvinnuleysi"),
        31: (18861.4, "Húsnæðis-og skipulagsmál"),
        32: (11297.6, "Lýðheilsa og stjórnsýsla velferðarmála"),
        33: (140907.9, "Fjármagnskostnaður, ábyrgðir og lífeyrisskuldbindi"),
        34: (57487.1, "Almennur varasjóður og sértækar fjárráðstafanir"),
        35: (12887.3, "Alþjóðleg þróunarsamvinna"),
    }

    result_rows = []
    for area_num, (amount, area_name) in heildarfjarhemild_2023.items():
        result_rows.append({
            "year": 2023,
            "malefnasvid_nr": f"{area_num:02d}",
            "malefnasvid": area_name,
            "source_type": "bill_approved",
            "document_id": "bill_2023_approved",
            "amount": amount,
        })

    logger.info(f"  Extracted {len(result_rows)} records from 2023 approved bill")
    return pd.DataFrame(result_rows)


def extract_2024_approved(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract 2024 approved budget bill data from PDF.

    2024 has rotated table on pages 6-7 with "Heildarfjárheimild" column.
    Manual values extracted from pages 6-7 (5th column in the table).
    """
    logger.info(f"  Using 2024-specific extractor for {file_path.name}")

    # Manually extracted "Heildarfjárheimild" values from 2024 approved bill
    # These are the definitive values from pages 6-7 (5th column)
    heildarfjarhemild_2024 = {
        1: (6771.8, "Alþingi og eftirlitsstofnanir þess"),
        2: (4044.8, "Dómstólar"),
        3: (2781.5, "Æðsta stjórnsýsla"),
        4: (15840.7, "Utanríkismál"),
        5: (29443.6, "Skatta-, eigna- og fjármálaumsýsla"),
        6: (3468.6, "Hagskýrslugerð og grunnskrár"),
        7: (32946.2, "Nýsköpun, rannsóknir og þekkingargreinar"),
        8: (33794.7, "Sveitarfélög og byggðamál"),
        9: (40400.9, "Almanna- og réttaröryggi"),
        10: (26423.7, "Rétt. einstakl., trúmál og stjórnsýsla dómsmála"),
        11: (56686.4, "Samgöngu- og fjarskiptamál"),
        12: (23390.3, "Landbúnaður"),
        13: (7752.0, "Sjávarútvegur og fiskeldi"),
        14: (2167.9, "Ferðaþjónusta"),
        15: (14670.5, "Orkumál"),
        16: (3969.4, "Markaðseftirlit og neytendamál"),
        17: (33647.4, "Umhverfismál"),
        18: (21822.1, "Menning, listir, íþrótta- og æskulýðsmál"),
        19: (6945.6, "Fjölmiðlun"),
        20: (45676.0, "Framhaldsskólastig"),
        21: (65403.3, "Háskólastig"),
        22: (5700.9, "Önnur skólastig og stjórnsýsla mennta- og barnamála"),
        23: (160993.4, "Sjúkrahúsþjónusta"),
        24: (89048.3, "Heilbrigðisþjónusta utan sjúkrahúsa"),
        25: (79851.1, "Hjúkrunar- og endurhæfingarþjónusta"),
        26: (41831.0, "Lyf og lækningavörur"),
        27: (107782.7, "Örorka og málefni fatlaðs fólks"),
        28: (117862.5, "Málefni aldraðra"),
        29: (63115.1, "Fjölskyldumál"),
        30: (47000.3, "Vinnumarkaður og atvinnuleysi"),
        31: (24532.4, "Húsnæðis- og skipulagsmál"),
        32: (11929.4, "Lýðheilsa og stjórnsýsla velferðarmála"),
        33: (179195.4, "Fjármagnskostnaður, ábyrgðir og lífeyrisskuldbindi"),
        34: (70905.7, "Almennur varasjóður og sértækar fjárráðstafanir"),
        35: (13106.8, "Alþjóðleg þróunarsamvinna"),
    }

    result_rows = []
    for area_num, (amount, area_name) in heildarfjarhemild_2024.items():
        result_rows.append({
            "year": 2024,
            "malefnasvid_nr": f"{area_num:02d}",
            "malefnasvid": area_name,
            "source_type": "bill_approved",
            "document_id": "bill_2024_approved",
            "amount": amount,
        })

    logger.info(f"  Extracted {len(result_rows)} records from 2024 approved bill")
    return pd.DataFrame(result_rows)


def extract_2025_approved(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract 2025 approved budget bill data from PDF.

    2025 has rotated table on pages 5-6 with "Heildarfjárheimild" column.
    Manual values extracted from pages 5-6 (5th column in the table).
    """
    logger.info(f"  Using 2025-specific extractor for {file_path.name}")

    # Manually extracted "Heildarfjárheimild" values from 2025 approved bill
    # These are the definitive values from pages 5-6 (5th column)
    heildarfjarhemild_2025 = {
        1: (6918.0, "Alþingi og eftirlitsstofnanir þess"),
        2: (4273.4, "Dómstólar"),
        3: (3098.2, "Æðsta stjórnsýsla"),
        4: (19225.8, "Utanríkismál"),
        5: (31199.7, "Skatta-, eigna- og fjármálaumsýsla"),
        6: (3360.5, "Hagskýrslugerð og grunnskrár"),
        7: (37697.1, "Nýsköpun, rannsóknir og þekkingargreinar"),
        8: (36803.2, "Sveitarfélög og byggðamál"),
        9: (44467.8, "Almanna- og réttaröryggi"),
        10: (26160.8, "Rétt. einstakl., trúmál og stjórnsýsla dómsmála"),
        11: (66128.0, "Samgöngu- og fjarskiptamál"),
        12: (24419.9, "Landbúnaður"),
        13: (8289.6, "Sjávarútvegur og fiskeldi"),
        14: (2417.3, "Ferðaþjónusta"),
        15: (13820.7, "Orkumál"),
        16: (4216.6, "Markaðseftirlit og neytendamál"),
        17: (38148.2, "Umhverfismál"),
        18: (25283.4, "Menning, listir, íþrótta- og æskulýðsmál"),
        19: (7149.2, "Fjölmiðlun"),
        20: (49118.7, "Framhaldsskólastig"),
        21: (69454.0, "Háskólastig"),
        22: (7035.0, "Önnur skólastig og stjórnsýsla mennta- og barnamála"),
        23: (174322.9, "Sjúkrahúsþjónusta"),
        24: (97067.5, "Heilbrigðisþjónusta utan sjúkrahúsa"),
        25: (87561.6, "Hjúkrunar- og endurhæfingarþjónusta"),
        26: (44147.4, "Lyf og lækningavörur"),
        27: (112523.2, "Örorka og málefni fatlaðs fólks"),
        28: (121569.9, "Málefni aldraðra"),
        29: (75711.0, "Fjölskyldumál"),
        30: (43863.0, "Vinnumarkaður og atvinnuleysi"),
        31: (27851.1, "Húsnæðis- og skipulagsmál"),
        32: (12521.0, "Lýðheilsa og stjórnsýsla velferðarmála"),
        33: (180772.8, "Fjármagnskostnaður, ábyrgðir og lífeyrisskuldbindi"),
        34: (24032.7, "Almennur varasjóður og sértækar fjárráðstafanir"),
        35: (15010.3, "Alþjóðleg þróunarsamvinna"),
    }

    result_rows = []
    for area_num, (amount, area_name) in heildarfjarhemild_2025.items():
        result_rows.append({
            "year": 2025,
            "malefnasvid_nr": f"{area_num:02d}",
            "malefnasvid": area_name,
            "source_type": "bill_approved",
            "document_id": "bill_2025_approved",
            "amount": amount,
        })

    logger.info(f"  Extracted {len(result_rows)} records from 2025 approved bill")
    return pd.DataFrame(result_rows)


def extract_2026_approved(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Extract 2026 approved budget bill data from PDF.

    2026 has rotated table on pages 5-6 with "Heildarfjárheimild" column.
    Manual values extracted from pages 5-6 (5th column in the table).
    """
    logger.info(f"  Using 2026-specific extractor for {file_path.name}")

    # Manually extracted "Heildarfjárheimild" values from 2026 approved bill
    # These are the definitive values from pages 5-6 (5th column)
    heildarfjarhemild_2026 = {
        1: (7005.2, "Alþingi og eftirlitsstofnanir þess"),
        2: (4605.8, "Dómstólar"),
        3: (3239.1, "Æðsta stjórnsýsla"),
        4: (23613.6, "Utanríkismál"),
        5: (30383.7, "Skatta-, eigna- og fjármálaumsýsla"),
        6: (3558.9, "Hagskýrslugerð og grunnskrár"),
        7: (34260.7, "Nýsköpun, rannsóknir og þekkingargreinar"),
        8: (40111.1, "Sveitarfélög og byggðamál"),
        9: (50305.2, "Almanna- og réttaröryggi"),
        10: (24006.5, "Rétt. einstakl., trúmál og stjórnsýsla dómsmála"),
        11: (76231.6, "Samgöngu- og fjarskiptamál"),
        12: (25913.3, "Landbúnaður"),
        13: (8964.1, "Sjávarútvegur og fiskeldi"),
        14: (2482.8, "Ferðaþjónusta"),
        15: (11945.1, "Orkumál"),
        16: (4848.9, "Markaðseftirlit og neytendamál"),
        17: (40424.4, "Umhverfismál"),
        18: (24466.8, "Menning, listir, íþrótta- og æskulýðsmál"),
        19: (7526.6, "Fjölmiðlun"),
        20: (53361.3, "Framhaldsskólastig"),
        21: (73365.9, "Háskólastig"),
        22: (7142.0, "Önnur skólastig og stjórnsýsla mennta- og barnamála"),
        23: (192567.1, "Sjúkrahúsþjónusta"),
        24: (104233.9, "Heilbrigðisþjónusta utan sjúkrahúsa"),
        25: (91796.0, "Hjúkrunar- og endurhæfingarþjónusta"),
        26: (44593.0, "Lyf og lækningavörur"),
        27: (141293.6, "Örorka og málefni fatlaðs fólks"),
        28: (125790.1, "Málefni aldraðra"),
        29: (86181.0, "Fjölskyldumál"),
        30: (49200.4, "Vinnumarkaður og atvinnuleysi"),
        31: (25913.1, "Húsnæðis- og skipulagsmál"),
        32: (14493.5, "Lýðheilsa og stjórnsýsla velferðarmála"),
        33: (210378.6, "Fjármagnskostnaður, ábyrgðir og lífeyrisskuldbindi"),
        34: (24618.4, "Almennur varasjóður og sértækar fjárráðstafanir"),
        35: (16372.5, "Alþjóðleg þróunarsamvinna"),
    }

    result_rows = []
    for area_num, (amount, area_name) in heildarfjarhemild_2026.items():
        result_rows.append({
            "year": 2026,
            "malefnasvid_nr": f"{area_num:02d}",
            "malefnasvid": area_name,
            "source_type": "bill_approved",
            "document_id": "bill_2026_approved",
            "amount": amount,
        })

    logger.info(f"  Extracted {len(result_rows)} records from 2026 approved bill")
    return pd.DataFrame(result_rows)


def extract_generic_approved(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Generic extractor for approved budget bills (fallback for other years).

    Looks for "heildargjöld" lines and extracts area numbers and amounts.
    """
    match = re.search(r"bill_(\d{4})", file_path.name)
    if not match:
        logger.warning(f"Could not parse year from filename: {file_path.name}")
        return None

    year = int(match.group(1))
    doc_id = f"bill_{year}_approved"

    result_rows = []

    try:
        with pdfplumber.open(file_path) as pdf:
            full_text = ""

            # Extract text from all pages
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    full_text += text + "\n"

            lines = full_text.split('\n')

            # Process all lines, handling rotated text
            for line in lines:
                original_line = line

                # Detect and reverse rotated text
                if any(reversed_word in line.lower() for reversed_word in
                       ['dlöjgradlieh', 'gitsalóks', 'atsunójþsúhark', 'runnurgrartskeR']):
                    line = line[::-1]

                if 'heildargjöld' not in line.lower():
                    continue

                # Skip header line
                if line.strip() == 'Heildargjöld':
                    continue

                # Extract area number and amount
                match = re.search(
                    r'^(\d+)\s+([^,]+),\s*heildargjöld\s*\.+\s*([\d.,]+)',
                    line
                )

                if match:
                    try:
                        area_num_str = match.group(1).strip()
                        area_num = int(area_num_str)

                        # Only include valid policy areas (1-35)
                        if not (1 <= area_num <= 35):
                            continue

                        # Parse amount (handle Icelandic format)
                        amount_str = match.group(3).strip()
                        amount = float(amount_str.replace('.', '').replace(',', '.'))

                        # Get policy area name
                        area_name = match.group(2).strip()

                        result_rows.append({
                            "year": year,
                            "malefnasvid_nr": area_num_str,
                            "malefnasvid": area_name,
                            "source_type": "bill_approved",
                            "document_id": doc_id,
                            "amount": amount,
                        })
                    except (ValueError, IndexError) as e:
                        logger.debug(f"Failed to parse line: {original_line}: {e}")
                        continue

        if result_rows:
            logger.info(f"  Extracted {len(result_rows)} records")
            df = pd.DataFrame(result_rows)
            # Remove duplicates (keep first occurrence of each area)
            df = df.drop_duplicates(subset=['malefnasvid_nr'], keep='first')
            return df
        else:
            logger.warning(f"  No records extracted from {file_path.name}")
            return None

    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        return None


def extract_from_pdf(file_path: Path) -> Optional[pd.DataFrame]:
    """
    Main entry point for PDF extraction.
    Routes to year-specific extractors based on filename.
    """
    match = re.search(r"bill_(\d{4})", file_path.name)
    if not match:
        logger.warning(f"Could not parse year from filename: {file_path.name}")
        return None

    year = int(match.group(1))

    # Route to year-specific extractor
    if year == 2020:
        return extract_2020_approved(file_path)
    elif year == 2021:
        return extract_2021_approved(file_path)
    elif year == 2022:
        return extract_2022_approved(file_path)
    elif year == 2023:
        return extract_2023_approved(file_path)
    elif year == 2024:
        return extract_2024_approved(file_path)
    elif year == 2025:
        return extract_2025_approved(file_path)
    elif year == 2026:
        return extract_2026_approved(file_path)
    else:
        # Use generic extractor for other years
        logger.info(f"  Using generic extractor for {year}")
        return extract_generic_approved(file_path)


def process_all_approved_bills() -> None:
    """Process all approved budget bill PDF files."""
    pdf_files = find_pdf_files()

    if not pdf_files:
        logger.warning(f"No PDF files found in {LANDING_DIR}")
        return

    logger.info(f"Found {len(pdf_files)} approved bill PDF files to process")

    processed_count = 0

    for file_path in pdf_files:
        df = extract_from_pdf(file_path)

        if df is not None and not df.empty:
            # Determine output filename
            match = re.search(r"bill_(\d{4})", file_path.name)
            if match:
                year = match.group(1)
                output_file = PROCESSED_DIR / f"bill_{year}_approved_malefnasvid.parquet"

                # Write to parquet
                df.to_parquet(output_file, compression="snappy")
                logger.info(f"  Saved: {output_file.name}")
                processed_count += 1

    logger.info(f"\nProcessed {processed_count}/{len(pdf_files)} approved bill files successfully")


if __name__ == "__main__":
    process_all_approved_bills()
