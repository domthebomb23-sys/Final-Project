#!/usr/bin/env python3
"""Geocode missing disaster rows using Nominatim (OpenStreetMap).

This script loads `disasters.db` and the existing `data/disasters_geo.json`,
finds rows that were not previously geocoded, queries Nominatim for each
location (one request per second to respect usage policy), and appends any
successful results to the geojson file.

Note: Nominatim requires a valid User-Agent. Edit the `USER_AGENT` variable
to include your contact email or app name if you publish this script.
"""
import sqlite3
import json
import time
import requests
from pathlib import Path
from urllib.parse import quote_plus

DB_PATH = Path('disasters.db')
GEOJSON_PATH = Path('data/disasters_geo.json')

# Provide a polite User-Agent identifying this script. Replace with your email
# if you plan to run many queries or publish the script.
USER_AGENT = 'FinalProjectGeocoder/1.0 (contact: final-project@example.com)'

def load_existing_ids():
    if not GEOJSON_PATH.exists():
        return set(), []
    data = json.loads(GEOJSON_PATH.read_text(encoding='utf-8'))
    features = data.get('features', [])
    ids = {f.get('properties', {}).get('id') for f in features}
    return ids, features

def query_nominatim(q):
    url = 'https://nominatim.openstreetmap.org/search?format=json&limit=1&q=' + quote_plus(q)
    headers = {'User-Agent': USER_AGENT}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            arr = resp.json()
            if arr:
                return float(arr[0]['lat']), float(arr[0]['lon'])
    except Exception as e:
        print('Request error for', q, e)
    return None

def main():
    if not DB_PATH.exists():
        print('disasters.db not found')
        return
    if not GEOJSON_PATH.exists():
        print('Existing geojson not found; run generate_geojson.py first.')
        return

    existing_ids, features = load_existing_ids()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # get columns to find location/disaster/year/article
    cur.execute("PRAGMA table_info(disasters)")
    cols_info = cur.fetchall()
    col_names = [c[1] for c in cols_info]
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
        print('No location-like column found in database')
        return

    cur.execute('SELECT rowid, * FROM disasters')
    rows = cur.fetchall()
    header = ['rowid'] + col_names
    idx = {name: i for i, name in enumerate(header)}

    to_geocode = []
    for r in rows:
        rid = r[0]
        if rid in existing_ids:
            continue
        loc_text = r[idx[col_location]] or ''
        loc_text = str(loc_text).strip()
        if not loc_text:
            continue
        to_geocode.append((rid, loc_text, r))

    print(f'Found {len(to_geocode)} rows to geocode')
    added = 0
    for i, (rid, loc_text, row) in enumerate(to_geocode, start=1):
        # Prefer to hint USA to Nominatim for ambiguous names
        query = loc_text
        if 'united states' not in query.lower() and 'usa' not in query.lower() and 'puerto rico' not in query.lower():
            query = f"{loc_text}, United States"
        print(f'[{i}/{len(to_geocode)}] Geocoding: {query}')
        coord = query_nominatim(query)
        if coord:
            lat, lon = coord
            props = {
                'id': rid,
                'year': row[idx[col_year]] if col_year else None,
                'disaster': row[idx[col_disaster]] if col_disaster else None,
                'article': row[idx[col_article]] if col_article else None,
                'location': loc_text,
            }
            feat = {
                'type': 'Feature',
                'geometry': {'type': 'Point', 'coordinates': [lon, lat]},
                'properties': props
            }
            features.append(feat)
            existing_ids.add(rid)
            added += 1
            print(f'  -> ok ({lat:.4f}, {lon:.4f})')
        else:
            print('  -> not found')
        # Respect rate limit: 1 request per second
        time.sleep(1.1)

    # Write back
    geo = {'type': 'FeatureCollection', 'features': features}
    GEOJSON_PATH.write_text(json.dumps(geo, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'Added {added} new features. Wrote {len(features)} features total to {GEOJSON_PATH}')

if __name__ == '__main__':
    main()
