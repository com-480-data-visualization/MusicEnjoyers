"""Aggregate track_nodupe CSVs into per-year duration stats for the website."""

import csv
import json
import os

NODUPE_DIR = os.path.join(
    os.path.dirname(__file__), "..",
    "Scrapper", "results", "track_nodupe"
)
OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "docs", "data", "duration_stats.json"
)

results = []

for year in range(2016, 2026):
    filename = f"track_ids_nd_{year}.csv"
    filepath = os.path.join(NODUPE_DIR, filename)
    if not os.path.exists(filepath):
        print(f"Warning: {filename} not found, skipping")
        continue

    durations = []  # (duration_ms, track_name, artist)
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                dur = int(float(row["Duration (ms)"]))
            except (ValueError, KeyError):
                continue
            track = row.get("Track Name", "Unknown")
            artist = row.get("Artist Name(s)", "Unknown")
            durations.append((dur, track, artist))

    if not durations:
        continue

    avg_ms = sum(d[0] for d in durations) / len(durations)
    shortest = min(durations, key=lambda d: d[0])
    longest = max(durations, key=lambda d: d[0])

    results.append({
        "year": year,
        "avg_seconds": round(avg_ms / 1000, 1),
        "count": len(durations),
        "min_seconds": round(shortest[0] / 1000, 1),
        "min_track": shortest[1],
        "min_artist": shortest[2],
        "max_seconds": round(longest[0] / 1000, 1),
        "max_track": longest[1],
        "max_artist": longest[2],
    })

results.sort(key=lambda r: r["year"])

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"Wrote {len(results)} years to {OUTPUT_PATH}")
for r in results:
    print(f"  {r['year']}: avg={r['avg_seconds']}s  n={r['count']}")
