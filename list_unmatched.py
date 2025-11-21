#!/usr/bin/env python3
"""List rows from disasters.db whose location text cannot be mapped by
generate_geojson.find_location_coords.
"""
import sqlite3
from pathlib import Path
import generate_geojson as gen

DB_PATH = Path('disasters.db')

def main():
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
    cur.execute('SELECT rowid, * FROM disasters')
    rows = cur.fetchall()
    header = ['rowid'] + cols
    idx = {name:i for i,name in enumerate(header)}
    unmatched = []
    for r in rows:
        loc = r[idx[col_location]]
        coord = None
        if loc:
            coord = gen.find_location_coords(str(loc))
        else:
            # try main_article and notes as fallback
            candidates = []
            for alt in ('main_article', 'notes'):
                if alt in idx:
                    val = r[idx[alt]]
                    if val:
                        candidates.append(str(val))
            combined = ' '.join(candidates).strip()
            if combined:
                coord = gen.find_location_coords(combined)
        if coord is None:
            unmatched.append((r[0], loc, r[idx[col_year]] if col_year else None, r[idx[col_disaster]] if col_disaster else None))
    print(f'Found {len(unmatched)} unmatched rows:')
    for u in unmatched:
        print(u)

if __name__ == '__main__':
    main()
