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
            stat = []
            for i in stat_year: 
                stat.append((i[label], i["track"], i["artist"]))
            avg_stat = round(sum(d[0] for d in stat) / len(stat), 3)
            if label == "Duration":
                avg_stat = int(avg_stat)
            shortest = min(stat, key=lambda d: d[0])
            longest = max(stat, key=lambda d: d[0])

            tmp = {
                f"avg_{label.lower()}": (avg_stat),
                f"min_{label.lower()}": round(shortest[0] / 1000, 1),
                f"min_track_{label.lower()}": shortest[1],
                f"min_artist_{label.lower()}": shortest[2],
                f"max_{label.lower()}": round(longest[0] / 1000, 1),
                f"max_track_{label.lower()}": longest[1],
                f"max_artist_{label.lower()}": longest[2],
            }
            res = res | tmp
        results.append(res)

results.sort(key=lambda r: r["year"])

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"Wrote {len(results)} years to {OUTPUT_PATH}")
for r in results:
    print(f"  {r['year']}: avg={r['avg_seconds']}s  n={r['count']}")
