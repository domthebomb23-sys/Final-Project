#!/usr/bin/env python3
"""Geocode unique location strings using Nominatim with caching.

This script:
 - Reads `disasters.db` and extracts a location query for each row (location or fallback fields).
 - Loads/updates a cache file `data/geocode_cache.json` mapping query -> {lat, lon}
 - Queries Nominatim for cache misses (rate-limited: 1 request/sec) using a polite User-Agent.
 - Writes updated cache and produces `data/disasters_geo.json` with a Feature per row using cached coordinates.

Usage: python3 geocode_nominatim.py

Note: this performs HTTP requests to nominatim.openstreetmap.org. We respect their usage policy by
sending a clear User-Agent and sleeping >=1s between requests. If you plan heavy use, provide contact info
in USER_AGENT or run your own Nominatim instance.
"""
import sqlite3
import json
import time
import requests
from pathlib import Path
from urllib.parse import quote_plus
import re
import generate_geojson as gen

DB_PATH = Path('disasters.db')
OUT_DIR = Path('data')
CACHE_PATH = OUT_DIR / 'geocode_cache.json'
OUT_PATH = OUT_DIR / 'disasters_geo.json'

# REQUIRED: update this to include contact info if you publish the script
USER_AGENT = 'FinalProjectGeocoder/1.0 (contact: final-project@example.com)'

OUT_DIR.mkdir(exist_ok=True)

def normalize_query(s: str) -> str:
    if not s:
        return ''
    s = s.strip()
    s = re.sub(r'\s+', ' ', s)
    s = s.replace('\n', ' ')
    return s

def load_cache():
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding='utf-8'))
    return {}

def save_cache(cache):
    CACHE_PATH.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding='utf-8')

def query_nominatim(q: str):
    url = 'https://nominatim.openstreetmap.org/search?format=json&limit=1&q=' + quote_plus(q)
    headers = {'User-Agent': USER_AGENT}
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            arr = r.json()
            if arr:
                return float(arr[0]['lat']), float(arr[0]['lon'])
    except Exception as e:
        print('Nominatim request error for', q, e)
    return None

def main():
    if not DB_PATH.exists():
        print('disasters.db not found; run csv_to_db.py first')
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(disasters)")
    cols = [c[1] for c in cur.fetchall()]

    def find_col(key):
        for c in cols:
            if key in c:
                return c
        return None

    col_location = find_col('location') or find_col('loc')
    col_year = find_col('year') or find_col('date')
    col_disaster = find_col('disaster')
    col_article = find_col('main_article') or find_col('article')
    col_notes = find_col('notes')

    if not col_location:
        print('No location-like column found in disasters table')
        return

    cur.execute('SELECT rowid, * FROM disasters')
    rows = cur.fetchall()
    header = ['rowid'] + cols
    idx = {name: i for i, name in enumerate(header)}

    # Build unique queries for rows
    queries = {}
    for r in rows:
        rid = r[0]
        loc = r[idx[col_location]]
        if loc and str(loc).strip():
            q = normalize_query(str(loc))
        else:
            # fallback to article or notes
            cand = []
            if col_article and r[idx[col_article]]:
                cand.append(str(r[idx[col_article]]))
            if col_notes and r[idx[col_notes]]:
                cand.append(str(r[idx[col_notes]]))
            q = normalize_query(' '.join(cand))
        # hint USA for ambiguous queries unless they mention other countries
        if q and 'united states' not in q.lower() and 'usa' not in q.lower() and 'puerto rico' not in q.lower() and 'venezuela' not in q.lower() and 'american samoa' not in q.lower():
            q = q + ', United States'
        queries[rid] = q

    cache = load_cache()

    # Determine which queries are missing from cache
    unique_qs = {}
    for rid, q in queries.items():
        if not q:
            continue
        unique_qs[q] = unique_qs.get(q, 0) + 1

    missing = [q for q in unique_qs.keys() if q not in cache]
    print(f'Total unique queries: {len(unique_qs)}; cache has {len(cache)} entries; {len(missing)} missing')

    # Helper to try multiple variants for a query
    def try_geocode_variants(q):
        # Try original
        if not q:
            return None
        attempts = [q]
        # try without trailing ', United States'
        if q.endswith(', United States'):
            attempts.append(q.replace(', United States', ''))
        # split on commas and try parts (most specific first)
        parts = [p.strip() for p in re.split(r',|;| and | & ', q) if p.strip()]
        # try longest parts first
        parts_sorted = sorted(parts, key=lambda x: -len(x))
        attempts.extend(parts_sorted)
        # try adding 'United States' to parts too
        attempts.extend([p + ', United States' for p in parts_sorted])
        # dedupe while preserving order
        seen = set()
        out = []
        for a in attempts:
            if a and a not in seen:
                seen.add(a)
                out.append(a)
        for a in out:
            res = query_nominatim(a)
            if res:
                return res
            # small delay between quick retries is handled in main loop
        return None

    # Query Nominatim for missing items with rate limit, but fall back to heuristic coords
    for i, q in enumerate(missing, start=1):
        print(f'[{i}/{len(missing)}] Geocoding variants for: {q}')
        res = try_geocode_variants(q)
        if res:
            lat, lon = res
            cache[q] = {'lat': lat, 'lon': lon}
            print(f'  -> ok {lat:.6f}, {lon:.6f}')
        else:
            cache[q] = None
            print('  -> no result from Nominatim; will fall back to approximate mapping later')
        # be polite
        time.sleep(1.1)

    # Save cache
    save_cache(cache)

    # Load existing approximate geojson (if available) to use as fallback
    approx_map = {}
    approx_path = Path('data') / 'disasters_geo.json'
    if approx_path.exists():
        try:
            approx = json.loads(approx_path.read_text(encoding='utf-8'))
            for f in approx.get('features', []):
                pid = f.get('properties', {}).get('id')
                if pid is not None:
                    coords = f.get('geometry', {}).get('coordinates')
                    if coords:
                        approx_map[pid] = (coords[1], coords[0])
        except Exception:
            pass

    # Build features using cached coordinates; fall back to approx_map or generate_geojson heuristic
    features = []
    skipped = 0
    for r in rows:
        rid = r[0]
        q = queries.get(rid)
        coord = None
        if q and q in cache and cache[q]:
            coord = (cache[q]['lat'], cache[q]['lon'])
        # fallback to approx file
        if coord is None and rid in approx_map:
            coord = approx_map[rid]
        # fallback to generate_geojson heuristic
        if coord is None:
            # try using the original location or article/notes via generate_geojson
            loc_val = queries.get(rid, '')
            coord = gen.find_location_coords(loc_val or '')
        if coord is None:
            skipped += 1
            continue
        props = {
            'id': rid,
            'year': r[idx[col_year]] if col_year else None,
            'disaster': r[idx[col_disaster]] if col_disaster else None,
            'article': r[idx[col_article]] if col_article else None,
            'location': r[idx[col_location]] if col_location else None,
        }
        features.append({
            'type': 'Feature',
            'geometry': {'type': 'Point', 'coordinates': [float(coord[1]), float(coord[0])]},
            'properties': props
        })

    geo = {'type': 'FeatureCollection', 'features': features}
    OUT_PATH.write_text(json.dumps(geo, indent=2, ensure_ascii=False), encoding='utf-8')
    print(f'Wrote {len(features)} features to {OUT_PATH} (skipped {skipped} rows). Cache saved to {CACHE_PATH}')


if __name__ == '__main__':
    main()
