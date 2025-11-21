#!/usr/bin/env python3
"""Convert `disasters.csv` into a SQLite database `disasters.db`.

Creates a table `disasters` with text columns derived from the CSV header.
This script keeps values as TEXT to avoid parsing edge cases; you can
add cleaning later if you want numeric aggregation.
"""
import csv
import sqlite3
import re
from pathlib import Path

CSV_PATH = Path('disasters.csv')
DB_PATH = Path('disasters.db')


def sanitize(col: str) -> str:
    # convert header to safe snake_case column name
    col = col.strip()
    col = col.replace('$', 'usd')
    col = re.sub(r"[^0-9a-zA-Z]+", '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_').lower()
    if not col:
        col = 'col'
    if col[0].isdigit():
        col = 'c_' + col
    return col


def main():
    if not CSV_PATH.exists():
        print(f"CSV not found at {CSV_PATH}. Run the scraper first to create it.")
        return

    with CSV_PATH.open(newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            print('CSV is empty')
            return

        cols = [sanitize(h) for h in header]

        # Create DB and table
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        # Use a simple schema: id + text columns for each header
        # Drop the table first so rerunning the script replaces previous data
        cur.execute('DROP TABLE IF EXISTS disasters')
        col_defs = ',\n'.join([f'"{c}" TEXT' for c in cols])
        create_sql = f"CREATE TABLE disasters (id INTEGER PRIMARY KEY, {col_defs});"
        cur.execute(create_sql)

        # Prepare insert statement
        placeholders = ','.join(['?'] * len(cols))
        insert_sql = f'INSERT INTO disasters ({",".join(["\""+c+"\"" for c in cols])}) VALUES ({placeholders})'

        rows = []
        count = 0
        for r in reader:
            # pad row to same length
            if len(r) < len(cols):
                r.extend([''] * (len(cols) - len(r)))
            rows.append([cell.strip() for cell in r])
            count += 1

        if rows:
            cur.executemany(insert_sql, rows)
            conn.commit()

        # Report
        cur.execute('SELECT COUNT(*) FROM disasters')
        total = cur.fetchone()[0]
        print(f'Inserted {count} rows into {DB_PATH} (table disasters). Total rows in table: {total}')

        conn.close()


if __name__ == '__main__':
    main()
