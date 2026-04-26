"""Aggregate track_nodupe CSVs into per-year genre stats for the website."""

import csv
import json
import os
from collections import Counter

NODUPE_DIR = os.path.join(
    os.path.dirname(__file__), "..",
    "Scrapper", "results", "track_nodupe"
)
OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "docs", "data", "genre_stats.json"
)

# 1. Identify Top 10 Genres
genre_counts = Counter()

for year in range(2016, 2026):
    filename = f"track_ids_nd_{year}.csv"
    filepath = os.path.join(NODUPE_DIR, filename)
    if not os.path.exists(filepath):
        continue

    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            genres_str = row.get("Genres", "")
            if not genres_str:
                continue
            
            # Genres are comma separated
            genres = [g.strip() for g in genres_str.split(",") if g.strip()]
            for g in genres:
                genre_counts[g] += 1

# Top 10 genres
top_10 = [g for g, c in genre_counts.most_common(10)]

# 2. Extract per-year data for Top 10 genres
results = []
for year in range(2016, 2026):
    filename = f"track_ids_nd_{year}.csv"
    filepath = os.path.join(NODUPE_DIR, filename)
    if not os.path.exists(filepath):
        continue

    year_genre_counts = Counter()
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            genres_str = row.get("Genres", "")
            if not genres_str:
                continue
            genres = [g.strip() for g in genres_str.split(",") if g.strip()]
            for g in genres:
                if g in top_10:
                    year_genre_counts[g] += 1

    year_data = {"year": year}
    for g in top_10:
        year_data[g] = year_genre_counts[g]
        
    results.append(year_data)

# Sort by year just in case
results.sort(key=lambda r: r["year"])

output_data = {
    "keys": top_10,
    "data": results
}

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(output_data, f, indent=2, ensure_ascii=False)

print(f"Wrote {len(results)} years to {OUTPUT_PATH}")
print(f"Top 10 Genres: {top_10}")
