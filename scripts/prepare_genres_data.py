"""Aggregate track_nodupe CSVs into genre stats, scatter, radar, and cluster data."""

import csv
import json
import os
from collections import Counter

import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from scipy.spatial.distance import cdist

NODUPE_DIR = os.path.join(
    os.path.dirname(__file__), "..",
    "Scrapper", "results", "track_nodupe"
)
GENRE_STATS_PATH = os.path.join(
    os.path.dirname(__file__), "..", "docs", "data", "genre_stats.json"
)
SCATTER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "docs", "data", "scatter_data.json"
)
CLUSTER_PATH = os.path.join(
    os.path.dirname(__file__), "..", "docs", "data", "cluster_stats.json"
)
RADAR_PATH = os.path.join(
    os.path.dirname(__file__), "..", "docs", "data", "radar_data.json"
)

SCATTER_FEAT_COLS = {
    "Danceability": "danceability",
    "Energy":       "energy",
    "Acousticness": "acousticness",
    "Valence":      "valence",
    "Speechiness":  "speechiness",
    "Tempo":        "tempo",
}

# ── Pass 1: single read of all CSVs ─────────────────────────────
# Collect genre counts, raw rows per year, and global tempo range.
genre_counts = Counter()
rows_by_year = {}
all_tempos   = []

for year in range(2016, 2026):
    filename = f"track_ids_nd_{year}.csv"
    filepath = os.path.join(NODUPE_DIR, filename)
    if not os.path.exists(filepath):
        continue

    year_rows = []
    with open(filepath, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            genres_str = row.get("Genres", "") or ""
            genres = [g.strip() for g in genres_str.split(",") if g.strip()]
            for g in genres:
                genre_counts[g] += 1
            try:
                all_tempos.append(float(row["Tempo"]))
            except (ValueError, KeyError):
                pass
            year_rows.append(row)

    rows_by_year[year] = year_rows

top_10 = [g for g, _ in genre_counts.most_common(10)]
t_min  = min(all_tempos) if all_tempos else 0.0
t_max  = max(all_tempos) if all_tempos else 1.0

os.makedirs(os.path.dirname(GENRE_STATS_PATH), exist_ok=True)

# ── Output 1: genre_stats.json ───────────────────────────────────
genre_results = []
for year, year_rows in sorted(rows_by_year.items()):
    counts = Counter()
    for row in year_rows:
        genres_str = row.get("Genres", "") or ""
        for g in [g.strip() for g in genres_str.split(",") if g.strip()]:
            if g in top_10:
                counts[g] += 1
    year_data = {"year": year}
    for g in top_10:
        year_data[g] = counts[g]
    genre_results.append(year_data)

with open(GENRE_STATS_PATH, "w", encoding="utf-8") as f:
    json.dump({"keys": top_10, "data": genre_results}, f, indent=2, ensure_ascii=False)
print(f"Wrote {len(genre_results)} years to {GENRE_STATS_PATH}")
print(f"Top 10 Genres: {top_10}")

# ── Output 2: scatter_data.json ──────────────────────────────────
scatter = {}
for year, year_rows in sorted(rows_by_year.items()):
    pts = []
    for row in year_rows:
        # Primary genre only; unmapped genres → "other"
        genres_str = row.get("Genres", "") or ""
        first_genre = genres_str.split(",")[0].strip().lower() if genres_str.strip() else ""
        genre = first_genre if first_genre in top_10 else "other"

        pt = {
            "title":  row.get("Track Name", ""),
            "artist": row.get("Artist Name(s)", ""),
            "genre":  genre,
        }

        valid = True
        for col, key in SCATTER_FEAT_COLS.items():
            try:
                pt[key] = round(float(row[col]), 4)
            except (ValueError, KeyError, TypeError):
                valid = False
                break
        if not valid:
            continue

        tempo_range    = t_max - t_min
        pt["tempo_norm"] = round((pt["tempo"] - t_min) / tempo_range, 4) if tempo_range else 0.0
        pts.append(pt)

    scatter[str(year)] = pts

with open(SCATTER_PATH, "w", encoding="utf-8") as f:
    json.dump(scatter, f, indent=2, ensure_ascii=False)
print(f"Wrote scatter data for {len(scatter)} years to {SCATTER_PATH}")
for yr, pts in sorted(scatter.items()):
    print(f"  {yr}: {len(pts)} points")

# ── Output 3: cluster_stats.json ─────────────────────────────────
CLUSTER_FEATS = ["Danceability", "Energy", "Tempo", "Valence", "Acousticness"]

cluster_results = []
for year, year_rows in sorted(rows_by_year.items()):
    X = []
    for row in year_rows:
        try:
            X.append([float(row[f]) for f in CLUSTER_FEATS])
        except (ValueError, KeyError, TypeError):
            pass
    if len(X) < 10:
        continue

    X_arr    = np.array(X)
    X_scaled = StandardScaler().fit_transform(X_arr)
    kmeans   = KMeans(n_clusters=5, random_state=42, n_init=10)
    labels   = kmeans.fit_predict(X_scaled)

    distances    = cdist(X_scaled, kmeans.cluster_centers_)
    avg_distance = float(np.mean([distances[i, c] for i, c in enumerate(labels)]))
    sil          = float(silhouette_score(X_scaled, labels))

    cluster_results.append({"year": year, "avg_distance": avg_distance, "silhouette": sil})

with open(CLUSTER_PATH, "w", encoding="utf-8") as f:
    json.dump(cluster_results, f, indent=2)
print(f"Wrote cluster stats for {len(cluster_results)} years to {CLUSTER_PATH}")

# ── Output 4: radar_data.json ────────────────────────────────────
RADAR_FEAT_COLS = {
    "Energy":       "energy",
    "Danceability": "danceability",
    "Acousticness": "acousticness",
    "Valence":      "valence",
    "Speechiness":  "speechiness",
}
RADAR_YEARS = [2016, 2025]

# Accumulate per-genre per-year feature values
accum = {}
for year, year_rows in rows_by_year.items():
    if year not in RADAR_YEARS:
        continue
    for row in year_rows:
        genres_str = row.get("Genres", "") or ""
        first = genres_str.split(",")[0].strip().lower() if genres_str.strip() else ""
        if first not in top_10:
            continue
        bucket = accum.setdefault(first, {}).setdefault(
            year, {key: [] for key in list(RADAR_FEAT_COLS.values()) + ["tempo"]}
        )
        valid = True
        for col, key in RADAR_FEAT_COLS.items():
            try:
                bucket[key].append(float(row[col]))
            except (ValueError, KeyError, TypeError):
                valid = False
                break
        if valid:
            try:
                bucket["tempo"].append(float(row["Tempo"]))
            except (ValueError, KeyError, TypeError):
                pass

def _mean(vals):
    return sum(vals) / len(vals) if vals else 0.0

radar = {"genres": [], "data": {}}
for g in top_10:
    if g not in accum:
        continue
    radar["genres"].append(g)
    radar["data"][g] = {}
    for yr in RADAR_YEARS:
        yr_data = accum[g].get(yr)
        if not yr_data:
            # fallback: merge all available years for this genre
            yr_data = {}
            for y_data in accum[g].values():
                for k, vals in y_data.items():
                    yr_data.setdefault(k, []).extend(vals)
        row_out = {key: round(_mean(yr_data.get(key, [])), 4)
                   for key in RADAR_FEAT_COLS.values()}
        t_range = t_max - t_min
        row_out["tempo_norm"] = round(
            (_mean(yr_data.get("tempo", [])) - t_min) / t_range, 4
        ) if t_range else 0.0
        radar["data"][g][str(yr)] = row_out

with open(RADAR_PATH, "w", encoding="utf-8") as f:
    json.dump(radar, f, indent=2, ensure_ascii=False)
print(f"Wrote radar data for {len(radar['genres'])} genres to {RADAR_PATH}")
