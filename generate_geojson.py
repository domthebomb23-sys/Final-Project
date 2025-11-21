#!/usr/bin/env python3
"""Generate a JSON file with approximate coordinates for each disaster row.

This script reads `disasters.db`, attempts to extract a location string for each
row, maps known US states and regions to approximate lat/lon centroids, and
outputs `data/disasters_geo.json` that the frontend can load.

The geocoding here is approximate and rule-based (no external API). It tries
to match state names/abbreviations and common region names; if no match is
found the row is skipped.
"""
import sqlite3
import json
import re
from pathlib import Path

DB_PATH = Path('disasters.db')
OUT_DIR = Path('data')
OUT_PATH = OUT_DIR / 'disasters_geo.json'

# Approximate centroids for US states and common regions (lat, lon)
# Source: approximate center coordinates
STATE_COORDS = {
    'alabama': (32.806671, -86.791130),
    'alaska': (61.370716, -152.404419),
    'arizona': (33.729759, -111.431221),
    'arkansas': (34.969704, -92.373123),
    'california': (36.116203, -119.681564),
    'colorado': (39.059811, -105.311104),
    'connecticut': (41.597782, -72.755371),
    'delaware': (39.318523, -75.507141),
    'florida': (27.766279, -81.686783),
    'georgia': (33.040619, -83.643074),
    'hawaii': (21.094318, -157.498337),
    'idaho': (44.240459, -114.478828),
    'illinois': (40.349457, -88.986137),
    'indiana': (39.849426, -86.258278),
    'iowa': (42.011539, -93.210526),
    'kansas': (38.526600, -96.726486),
    'kentucky': (37.668140, -84.670067),
    'louisiana': (31.169546, -91.867805),
    'maine': (44.693947, -69.381927),
    'maryland': (39.063946, -76.802101),
    'massachusetts': (42.230171, -71.530106),
    'michigan': (43.326618, -84.536095),
    'minnesota': (45.694454, -93.900192),
    'mississippi': (32.741646, -89.678696),
    'missouri': (38.456085, -92.288368),
    'montana': (46.921925, -110.454353),
    'nebraska': (41.125370, -98.268082),
    'nevada': (38.313515, -117.055374),
    'new hampshire': (43.452492, -71.563896),
    'new jersey': (40.298904, -74.521011),
    'new mexico': (34.840515, -106.248482),
    'new york': (42.165726, -74.948051),
    'north carolina': (35.630066, -79.806419),
    'north dakota': (47.528912, -99.784012),
    'ohio': (40.388783, -82.764915),
    'oklahoma': (35.565342, -96.928917),
    'oregon': (44.572021, -122.070938),
    'pennsylvania': (40.590752, -77.209755),
    'rhode island': (41.680893, -71.511780),
    'south carolina': (33.856892, -80.945007),
    'south dakota': (44.299782, -99.438828),
    'tennessee': (35.747845, -86.692345),
    'texas': (31.054487, -97.563461),
    'utah': (40.150032, -111.862434),
    'vermont': (44.045876, -72.710686),
    'virginia': (37.769337, -78.169968),
    'washington': (47.400902, -121.490494),
    'west virginia': (38.491226, -80.954453),
    'wisconsin': (44.268543, -89.616508),
    'wyoming': (42.755966, -107.302490),
    'district of columbia': (38.9072, -77.0369),
    'puerto rico': (18.2208, -66.5901),
}

# Some common region keywords mapped to approximate coordinates
REGION_COORDS = {
    'southern united states': (31.0, -86.0),
    'southern united states,midwestern united states': (36.0, -88.0),
    'midwestern united states': (41.5, -90.0),
    'western united states': (40.0, -120.0),
    'eastern united states': (40.0, -75.0),
    'northeastern united states': (43.0, -71.0),
    'southeastern united states': (32.0, -82.0),
    'great lakes region': (43.0, -84.0),
    'hawaii': STATE_COORDS.get('hawaii'),
    'alaska': STATE_COORDS.get('alaska'),
}


def find_location_coords(loc_text: str):
    if not loc_text:
        return None
    s = loc_text.lower()
    s = s.replace('\n', ' ')
    # normalize standalone 'and' (avoid breaking words like 'england')
    s = re.sub(r"\band\b", ' and ', s)
    # normalize dashes to a single ASCII hyphen
    s = s.replace('\u2013', '-')
    s = s.replace('\u2014', '-')
    s = s.replace('/', ', ')
    s = re.sub(r'[_\t]+', ' ', s)

    # remove extra punctuation
    s = re.sub(r'[\.\(\)\[\]]+', ' ', s)
    # normalize spaces around hyphens (e.g. 'south - central' -> 'south-central')
    s = re.sub(r'\s*-\s*', '-', s)
    s = re.sub(r'\s+', ' ', s).strip()

    # Try exact state name match
    for state, coord in STATE_COORDS.items():
        if state in s:
            return coord

    # Try matching by common abbreviations (e.g., "fl" or "tx")
    abbr_map = {v: k for k, v in {
        'AL': 'alabama', 'AK': 'alaska', 'AZ': 'arizona', 'AR': 'arkansas', 'CA': 'california',
        'CO': 'colorado', 'CT': 'connecticut', 'DE': 'delaware', 'FL': 'florida', 'GA': 'georgia',
        'HI': 'hawaii', 'ID': 'idaho', 'IL': 'illinois', 'IN': 'indiana', 'IA': 'iowa',
        'KS': 'kansas', 'KY': 'kentucky', 'LA': 'louisiana', 'ME': 'maine', 'MD': 'maryland',
        'MA': 'massachusetts', 'MI': 'michigan', 'MN': 'minnesota', 'MS': 'mississippi',
        'MO': 'missouri', 'MT': 'montana', 'NE': 'nebraska', 'NV': 'nevada', 'NH': 'new hampshire',
        'NJ': 'new jersey', 'NM': 'new mexico', 'NY': 'new york', 'NC': 'north carolina', 'ND': 'north dakota',
        'OH': 'ohio', 'OK': 'oklahoma', 'OR': 'oregon', 'PA': 'pennsylvania', 'RI': 'rhode island',
        'SC': 'south carolina', 'SD': 'south dakota', 'TN': 'tennessee', 'TX': 'texas', 'UT': 'utah',
        'VT': 'vermont', 'VA': 'virginia', 'WA': 'washington', 'WV': 'west virginia', 'WI': 'wisconsin', 'WY': 'wyoming'
    }.items()}
    # token-wise check for two-letter state codes
    tokens = re.split(r'[,\/;]+| and | & ', s)
    for tok in tokens:
        tok = tok.strip()
        if len(tok) == 2 and tok.upper() in abbr_map:
            state_name = abbr_map[tok.upper()]
            return STATE_COORDS.get(state_name)

    # Try region matches (added more synonyms)
    # expand REGION_COORDS with common variants
    expanded_regions = dict(REGION_COORDS)
    expanded_regions.update({
        'western north america': (45.0, -120.0),
        'east coast': (40.0, -73.0),
        'carolinas': (35.5, -79.0),
        'great plains': (39.0, -98.0),
        'great lakesarea': (43.0, -84.0),
        'northeast': (43.0, -71.0),
        'northwest': (46.5, -120.0),
        'south-central united states': (33.0, -95.0),
        'south central united states': (33.0, -95.0),
        'central and southern states': (33.0, -95.0),
        'midwestandnortheast': (41.0, -89.0),
        'midwest and northeast': (41.0, -89.0),
        'eastern us': (40.0, -75.0),
        'eastern united states': (40.0, -75.0),
        'western north america': (45.0, -120.0),
    })
    for region, coord in expanded_regions.items():
        if region in s:
            return coord

    # Additional direct keyword mappings for non-US places appearing in the CSV
    # and other phrases
    extra_keywords = {
        'caribbean': (15.0, -61.0),
        'venezuela': (7.0, -66.0),
        'yucatan': (20.5, -89.5),
        'yucatán': (20.5, -89.5),
        'american samoa': (-14.271, -170.132),
        'atlantic canada': (44.65, -63.6),
        'widespread': (39.8, -98.5),
        'nation': (39.8, -98.5),
        'new england': (42.5, -71.5),
        'great lakes': (43.0, -84.0),
        'la plata': (38.5, -76.97),
    }
    for k, v in extra_keywords.items():
        if k in s:
            return v

    # Try matching by city-like phrases (fallback keywords)
    # Common US cities that appear in the dataset
    city_map = {
        'texas': STATE_COORDS['texas'],
        'california': STATE_COORDS['california'],
        'los angeles': STATE_COORDS['california'],
        'greater los angeles': STATE_COORDS['california'],
        'florida': STATE_COORDS['florida'],
        'kansas': STATE_COORDS['kansas'],
        'montana': STATE_COORDS['montana'],
        'kentucky': STATE_COORDS['kentucky'],
        'new york': STATE_COORDS['new york'],
        'hawaii': STATE_COORDS['hawaii'],
        'puerto rico': STATE_COORDS['puerto rico'],
        'alaska': STATE_COORDS['alaska'],
        'fargo': STATE_COORDS['north dakota'],
        'waco': STATE_COORDS['texas'],
        'carolinas': (35.5, -79.0),
        'great plains': (39.0, -98.0),
        'great lakesarea': (43.0, -84.0),
        'midwest': (41.5, -90.0),
        'midwestern united states': (41.5, -90.0),
        'northwest': (46.5, -120.0),
        'northeast': (43.0, -71.0),
        'south central': (33.0, -95.0),
        'american samoa': (-14.271, -170.132),
        'caribbean': (15.0, -61.0),
        'venezuela': (7.0, -66.0),
        'yucatan': (20.5, -89.5),
        'yucatán': (20.5, -89.5),
        'new england': (42.5, -71.5),
        'great lakes': (43.0, -84.0),
    }
    for k, v in city_map.items():
        if k in s:
            return v

    return None


def main():
    if not DB_PATH.exists():
        print('disasters.db not found. Run csv_to_db.py first.')
        return

    OUT_DIR.mkdir(exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get columns
    cur.execute("PRAGMA table_info(disasters)")
    cols_info = cur.fetchall()
    col_names = [c[1] for c in cols_info]

    # Heuristics to find the important column names
    def find_col(key):
        for c in col_names:
            if key in c:
                return c
        return None

    col_location = find_col('location') or find_col('loc')
    col_year = find_col('year') or find_col('date')
    col_disaster = find_col('disaster')
    col_article = find_col('article')

    if not col_location:
        print('No location-like column found in the database. Columns:', col_names)
        return

    # Read rows
    cur.execute(f'SELECT rowid, * FROM disasters')
    rows = cur.fetchall()

    # map column indices
    header = ['rowid'] + col_names
    idx_map = {name: i for i, name in enumerate(header)}

    features = []
    skipped = 0
    for r in rows:
        loc_text = r[idx_map[col_location]]
        # if location is empty, try to use other text fields (main_article, notes)
        if not loc_text:
            candidates = []
            for alt in (col for col in ('main_article', 'notes') if col in idx_map):
                val = r[idx_map.get(alt)] if idx_map.get(alt) is not None else None
                if val:
                    candidates.append(str(val))
            combined = ' '.join(candidates).strip()
            coord = find_location_coords(combined or '')
        else:
            coord = find_location_coords(loc_text or '')
        if coord is None:
            skipped += 1
            continue

        props = {
            'id': r[0],
            'year': r[idx_map[col_year]] if col_year else None,
            'disaster': r[idx_map[col_disaster]] if col_disaster else None,
            'article': r[idx_map[col_article]] if col_article else None,
            'location': loc_text,
        }

        features.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [coord[1], coord[0]]  # lon, lat
            },
            'properties': props
        })

    geo = {
        'type': 'FeatureCollection',
        'features': features
    }

    with OUT_PATH.open('w', encoding='utf-8') as f:
        json.dump(geo, f, indent=2, ensure_ascii=False)

    print(f'Wrote {len(features)} features to {OUT_PATH} (skipped {skipped} rows)')


if __name__ == '__main__':
    main()
