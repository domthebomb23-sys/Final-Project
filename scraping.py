import requests
from bs4 import BeautifulSoup

url = "https://en.wikipedia.org/wiki/List_of_natural_disasters_in_the_United_States"

# Minimal beginner-friendly scraper:
resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
if not resp.ok:
    print("Request failed with status", resp.status_code)
    exit(1)

soup = BeautifulSoup(resp.content, 'html.parser')

# Find the first table with class 'wikitable'
table = soup.find('table', class_='wikitable')
if table is None:
    print("No table with class 'wikitable' found on the page.")
    exit(1)

rows = []
for tr in table.find_all('tr')[1:]:
    cols = [ele.get_text(strip=True) for ele in tr.find_all(['td', 'th'])]
    rows.append(cols)

print(f"Found {len(rows)} rows.")

# Create a header from the first table row (the table's header)
first_row = table.find('tr')
header = [ele.get_text(strip=True) for ele in first_row.find_all(['th', 'td'])]

# Ensure all rows have the same number of columns as the header
max_cols = max(len(header), max((len(r) for r in rows), default=0))
for r in rows:
    if len(r) < max_cols:
        r.extend([''] * (max_cols - len(r)))

if len(header) < max_cols:
    header.extend([''] * (max_cols - len(header)))

import csv

out_path = 'disasters.csv'
with open(out_path, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

print(f"Saved {len(rows)} rows (plus header) to {out_path}")

print("Sample (first 5 rows):")
for r in rows[:5]:
    print(r)