"""Aggregate track_nodupe CSVs into per-year duration stats for the website."""

import csv
import json
import os

NODUPE_DIR = os.path.join(
    os.path.dirname(__file__), "..",
    "Scrapper", "results", "track_nodupe"
)
OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "docs", "data", "general_stats.json"
)

results = []
METRICS = [
    "Duration",
    "BPM",
    "Loudness",
    "Danceability",
    "Speechiness",
    "Acousticness",
    "Energy" 
]

def normalize_reader(reader):
    for row in reader:
        if "Tempo" in row:
            try: 
                row["BPM"] = row["Tempo"]
            except ValueError:
                pass
        if "Duration (ms)" in row:
            try:
                row["Duration"] = int(float(row["Duration (ms)"]) / 1000)
            except ValueError:
                pass 
        yield row
result = []
for year in range(2016, 2026):
    filename = f"track_ids_nd_{year}.csv"
    filepath = os.path.join(NODUPE_DIR, filename)
    if not os.path.exists(filepath):
        print(f"Warning: {filename} not found, skipping")
        continue

    stat_year = []  # (duration_ms, track_name, artist)
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        reader = normalize_reader(reader)


        for row in reader:
            track = row.get("Track Name", "Unknown")
            artist = row.get("Artist Name(s)", "Unknown")
            tmp = {
                "artist"    : artist, 
                "track"     : track
            }
            for label in METRICS:
                try:
                    s = float(row[label])
                except (ValueError, KeyError):
                    continue
                tmp[label]  = round(s,3)
            stat_year.append(tmp)
        if not stat_year:
            continue
        res = {
            "year"  : year,
            "count": len(stat_year),
        }
        for label in METRICS:
            # Only include rows that actually have this metric value
            stat = [
                (i[label], i["track"], i["artist"])
                for i in stat_year if label in i
            ]
            if not stat:
                continue

            avg_stat = round(sum(d[0] for d in stat) / len(stat), 3)
            if label == "Duration":
                avg_stat = int(avg_stat)
            shortest = min(stat, key=lambda d: d[0])
            longest  = max(stat, key=lambda d: d[0])

            # Duration is already in seconds; all other metrics are raw floats
            # (0–1, dB, BPM) — do NOT divide by 1000.
            min_val = round(shortest[0], 3)
            max_val = round(longest[0], 3)

            key = label.lower()
            tmp = {
                f"avg_{key}":          avg_stat,
                f"min_{key}":          min_val,
                f"min_track_{key}":    shortest[1],
                f"min_artist_{key}":   shortest[2],
                f"max_{key}":          max_val,
                f"max_track_{key}":    longest[1],
                f"max_artist_{key}":   longest[2],
            }
            res = res | tmp
        results.append(res)

results.sort(key=lambda r: r["year"])

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"Wrote {len(results)} years to {OUTPUT_PATH}")
for r in results:
    print(f"  {r['year']}: avg_duration={r.get('avg_duration')}s  n={r['count']}")