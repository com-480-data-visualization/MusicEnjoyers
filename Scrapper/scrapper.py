"""
enrich_billboard.py

Reads a billboard CSV (with track_id as Spotify track ID), fetches track metadata
and audio features from the Spotify API via spotipy, and writes an enriched CSV.

Requires the following environment variables:
    SPOTIPY_CLIENT_ID
    SPOTIPY_CLIENT_SECRET
    SPOTIPY_REDIRECT_URI
"""
import csv
import json
import os
import re
import time
import unicodedata
import spotipy
from concurrent.futures import ThreadPoolExecutor, as_completed
from difflib import SequenceMatcher
from spotipy.oauth2 import SpotifyOAuth
from creds import hard_coded_creds

# ── Auth ──────────────────────────────────────────────────────────────────────
scope = "user-library-read"

sp = hard_coded_creds

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE  = "source_files/billboard_hot100_2025_monthly_enriched.csv"
OUTPUT_FILE       = "billboard_enriched.csv"
UNMATCHED_FILE    = "billboard_unmatched.csv"
CHECKPOINT_FILE   = "billboard_checkpoint.json"  # resume from here if interrupted
 
BATCH_SIZE        = 50    # Spotify API max for audio_features / artists
SEARCH_WORKERS    = 10    # concurrent search threads (stay well under rate limit)
MATCH_THRESHOLD   = 0.6   # minimum similarity score to accept a match
SEARCH_CANDIDATES = 5     # Spotify results to score per query
BACKOFF_RETRIES   = 3     # retries on rate-limit (429) errors
BACKOFF_BASE      = 2.0   # seconds; doubles each retry
 
OUTPUT_FIELDS = [
    "track_id", "artists", "album_name", "track_name",
    "popularity", "duration_ms", "explicit",
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature",
    "track_genre",
]
 
# ── Text normalisation ────────────────────────────────────────────────────────
 
def normalise(text: str) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    noise = (r"\b(remaster(ed)?|remix(ed)?|live|acoustic|version|edit|deluxe"
             r"|anniversary|mono|stereo|radio|single|extended|original)\b")
    text = re.sub(noise, "", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
 
 
def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalise(a), normalise(b)).ratio()
 
 
def combined_score(csv_track, csv_artist, sp_track, sp_artist) -> float:
    return 0.7 * similarity(csv_track, sp_track) + 0.3 * similarity(csv_artist, sp_artist)
 
# ── Spotify helpers ───────────────────────────────────────────────────────────
 
def _spotify_search_raw(query: str) -> list:
    """Call sp.search with exponential backoff on 429s."""
    delay = BACKOFF_BASE
    for attempt in range(BACKOFF_RETRIES):
        try:
            result = sp.search(q=query, type="track", limit=SEARCH_CANDIDATES)
            return result.get("tracks", {}).get("items", [])
        except spotipy.exceptions.SpotifyException as e:
            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", delay))
                print(f"    Rate limited — waiting {retry_after}s")
                time.sleep(retry_after)
                delay *= 2
            else:
                raise
    return []
 
 
def search_spotify(row: dict) -> tuple[dict, dict | None]:
    """
    Search Spotify for a single row. Returns (row, match_or_None).
    Designed to be called from a thread pool.
    """
    track_name = row.get("track_name", "").strip()
    artist     = row.get("artists", "").strip()
 
    # Structured query first, plain-text fallback
    candidates = _spotify_search_raw(f"track:{track_name} artist:{artist}")
    if not candidates:
        candidates = _spotify_search_raw(f"{track_name} {artist}")
 
    best_score = -1.0
    best_track = None
    for item in candidates:
        score = combined_score(
            track_name, artist,
            item["name"],
            ", ".join(a["name"] for a in item["artists"])
        )
        if score > best_score:
            best_score, best_track = score, item
 
    if best_score < MATCH_THRESHOLD or best_track is None:
        return row, None
 
    return row, {
        "spotify_id":  best_track["id"],
        "artists":     ", ".join(a["name"] for a in best_track["artists"]),
        "artist_ids":  [a["id"] for a in best_track["artists"]],
        "album_name":  best_track["album"]["name"],
        "track_name":  best_track["name"],
        "popularity":  best_track["popularity"],
        "duration_ms": best_track["duration_ms"],
        "explicit":    best_track["explicit"],
        "match_score": round(best_score, 3),
    }
 
 
def fetch_audio_features(spotify_ids: list[str]) -> dict:
    results = {}
    for batch in chunks(spotify_ids, BATCH_SIZE):
        for attempt in range(BACKOFF_RETRIES):
            try:
                response = sp.audio_features(batch)
                break
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    wait = int(e.headers.get("Retry-After", BACKOFF_BASE * (attempt + 1)))
                    time.sleep(wait)
                else:
                    raise
        for feat in (response or []):
            if feat is None:
                continue
            results[feat["id"]] = {k: feat[k] for k in (
                "danceability", "energy", "key", "loudness", "mode",
                "speechiness", "acousticness", "instrumentalness",
                "liveness", "valence", "tempo", "time_signature",
            )}
    return results
 
 
def fetch_artist_genres(artist_ids: list[str]) -> dict:
    results = {}
    for batch in chunks(artist_ids, BATCH_SIZE):
        for attempt in range(BACKOFF_RETRIES):
            try:
                response = sp.artists(batch)
                break
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:
                    wait = int(e.headers.get("Retry-After", BACKOFF_BASE * (attempt + 1)))
                    time.sleep(wait)
                else:
                    raise
        for artist in (response or {}).get("artists", []):
            if artist:
                genres = artist.get("genres", [])
                results[artist["id"]] = genres[0] if genres else ""
    return results
 
 
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
 
# ── Checkpoint helpers ────────────────────────────────────────────────────────
 
def load_checkpoint() -> dict:
    """Returns {track_id: match_or_null} for already-processed rows."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}
 
 
def save_checkpoint(checkpoint: dict):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(checkpoint, f)
 
# ── Main ──────────────────────────────────────────────────────────────────────
 
def main():
    # 1. Read input
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows)} tracks from {INPUT_FILE}")
 
    # 2. Resume from checkpoint
    checkpoint = load_checkpoint()
    pending = [r for r in rows if r["track_id"] not in checkpoint]
    print(f"  {len(checkpoint)} already processed, {len(pending)} remaining\n")
 
    # 3. Concurrent search
    done = 0
    with ThreadPoolExecutor(max_workers=SEARCH_WORKERS) as pool:
        futures = {pool.submit(search_spotify, row): row for row in pending}
        for future in as_completed(futures):
            row, match = future.result()
            checkpoint[row["track_id"]] = match   # None = no match
            done += 1
            status = (f"✓ score={match['match_score']} '{match['track_name']}'"
                      if match else "✗ no match")
            print(f"  [{done}/{len(pending)}] {row['track_name']} — {status}")
            # Save checkpoint every 50 completions
            if done % 50 == 0:
                save_checkpoint(checkpoint)
 
    save_checkpoint(checkpoint)
 
    # 4. Split matched / unmatched
    matched   = [(r, checkpoint[r["track_id"]]) for r in rows
                 if checkpoint.get(r["track_id"]) is not None]
    unmatched = [r for r in rows if checkpoint.get(r["track_id"]) is None]
    print(f"\nMatched {len(matched)} / {len(rows)} ({len(unmatched)} unmatched)")
 
    if not matched:
        print("No matches — nothing to write.")
        return
 
    # 5. Batch-fetch audio features
    spotify_ids = [m["spotify_id"] for _, m in matched]
    print("Fetching audio features...")
    audio_data = fetch_audio_features(spotify_ids)
 
    # 6. Batch-fetch genres
    all_artist_ids = list({aid for _, m in matched for aid in m["artist_ids"]})
    print("Fetching artist genres...")
    artist_genre_map = fetch_artist_genres(all_artist_ids)
 
    # 7. Build and write enriched output
    enriched_rows = []
    for row, match in matched:
        feats = audio_data.get(match["spotify_id"], {})
        genre = artist_genre_map.get(match["artist_ids"][0], "") if match["artist_ids"] else ""
        enriched_rows.append({
            "track_id":         row["track_id"],
            "artists":          match["artists"],
            "album_name":       match["album_name"],
            "track_name":       match["track_name"],
            "popularity":       match["popularity"],
            "duration_ms":      match["duration_ms"],
            "explicit":         match["explicit"],
            **feats,
            "track_genre":      genre,
        })
 
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(enriched_rows)
    print(f"\n✓ Enriched CSV  -> {OUTPUT_FILE} ({len(enriched_rows)} rows)")
 
    if unmatched:
        with open(UNMATCHED_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=unmatched[0].keys())
            writer.writeheader()
            writer.writerows(unmatched)
        print(f"  Unmatched CSV -> {UNMATCHED_FILE} ({len(unmatched)} rows)")
 
    # Clean up checkpoint on successful completion
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("  Checkpoint removed.")
 
 
if __name__ == "__main__":
    main()