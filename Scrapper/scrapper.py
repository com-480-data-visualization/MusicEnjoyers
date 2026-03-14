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
import os
import re
import time
import unicodedata
import spotipy
from difflib import SequenceMatcher
from spotipy.oauth2 import SpotifyOAuth


# ── Auth ──────────────────────────────────────────────────────────────────────
scope = "user-library-read"

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id='70f349e990e443f1a40645c7def01ee7',
        client_secret='c83d5ca153894174b27bb56b9117e0b1',
        redirect_uri='http://127.0.0.1:1234',
        scope=scope
    )
)

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_FILE  = "source_files/billboard_hot100_2025_monthly_enriched.csv"
OUTPUT_FILE       = "billboard_enriched.csv"
UNMATCHED_FILE    = "billboard_unmatched.csv"   # rows that couldn't be matched
BATCH_SIZE        = 50     # Spotify API max for tracks/audio_features
SLEEP_SEC         = 0.2    # polite delay between API calls
MATCH_THRESHOLD   = 0.6    # minimum combined similarity score to accept a match
SEARCH_CANDIDATES = 5      # how many Spotify results to score per query

OUTPUT_FIELDS = [
    "track_id", "artists", "album_name", "track_name",
     "duration_ms", "explicit",
    "danceability", "energy", "key", "loudness", "mode",
    "speechiness", "acousticness", "instrumentalness",
    "liveness", "valence", "tempo", "time_signature",
    "track_genre",
]
 
# ── Text normalisation ────────────────────────────────────────────────────────
 
def normalise(text: str) -> str:
    """
    Lowercase, strip accents, remove punctuation and common noise phrases
    so that e.g. "Rockin' Around The Christmas Tree" matches
    "Rockin' Around the Christmas Tree - Remastered 2022".
    """
    if not text:
        return ""
    # Unicode -> ASCII-safe (strip accents)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.lower()
    # Remove noise suffixes (remaster, remix, live, deluxe, etc.)
    noise = (r"\b(remaster(ed)?|remix(ed)?|live|acoustic|version|edit|deluxe"
             r"|anniversary|mono|stereo|radio|single|extended|original)\b")
    text = re.sub(noise, "", text)
    # Strip punctuation and extra whitespace
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
 
 
def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalise(a), normalise(b)).ratio()
 
 
def combined_score(csv_track: str, csv_artist: str,
                   sp_track: str,  sp_artist: str) -> float:
    """
    Weighted average: track name is more discriminating than artist name.
    """
    track_sim  = similarity(csv_track,  sp_track)
    artist_sim = similarity(csv_artist, sp_artist)
    return 0.7 * track_sim + 0.3 * artist_sim
 
# ── Spotify helpers ───────────────────────────────────────────────────────────
 
def search_spotify(track_name: str, artist: str) -> dict | None:
    """
    Search Spotify for track + artist, score all candidates, return the
    best match above MATCH_THRESHOLD or None.
    """
    query = f"track:{track_name} artist:{artist}"
    try:
        results = sp.search(q=query, type="track", limit=SEARCH_CANDIDATES)
    except Exception as e:
        print(f"    Search error for '{track_name}' / '{artist}': {e}")
        return None
 
    candidates = results.get("tracks", {}).get("items", [])
    if not candidates:
        # Fallback: plain text search (less precise but broader)
        try:
            results = sp.search(q=f"{track_name} {artist}", type="track",
                                limit=SEARCH_CANDIDATES)
            candidates = results.get("tracks", {}).get("items", [])
        except Exception as e:
            print(f"    Fallback search error: {e}")
            return None
 
    best_score = -1.0
    best_track = None
    for item in candidates:
        sp_track_name  = item["name"]
        sp_artist_name = ", ".join(a["name"] for a in item["artists"])
        score = combined_score(track_name, artist, sp_track_name, sp_artist_name)
        if score > best_score:
            best_score = score
            best_track = item
 
    if best_score < MATCH_THRESHOLD or best_track is None:
        return None
 
    return {
        "spotify_id":  best_track["id"],
        "artists":     ", ".join(a["name"] for a in best_track["artists"]),
        "artist_ids":  [a["id"] for a in best_track["artists"]],
        "album_name":  best_track["album"]["name"],
        "track_name":  best_track["name"],
        # "popularity":  best_track["popularity"],
        "duration_ms": best_track["duration_ms"],
        "explicit":    best_track["explicit"],
        "match_score": round(best_score, 3),
    }
 
 
def fetch_audio_features(spotify_ids: list[str]) -> dict:
    """Batch-fetch audio features. Returns dict keyed by spotify_id."""
    results = {}
    for batch in chunks(spotify_ids, BATCH_SIZE):
        response = sp.audio_features(batch)
        for feat in (response or []):
            if feat is None:
                continue
            results[feat["id"]] = {
                "danceability":     feat["danceability"],
                "energy":           feat["energy"],
                "key":              feat["key"],
                "loudness":         feat["loudness"],
                "mode":             feat["mode"],
                "speechiness":      feat["speechiness"],
                "acousticness":     feat["acousticness"],
                "instrumentalness": feat["instrumentalness"],
                "liveness":         feat["liveness"],
                "valence":          feat["valence"],
                "tempo":            feat["tempo"],
                "time_signature":   feat["time_signature"],
            }
        time.sleep(SLEEP_SEC)
    return results
 
 
def fetch_artist_genres(artist_ids: list[str]) -> dict:
    """Batch-fetch primary genre per artist. Returns dict keyed by artist_id."""
    results = {}
    for batch in chunks(artist_ids, BATCH_SIZE):
        response = sp.artists(batch)
        for artist in (response or {}).get("artists", []):
            if artist is None:
                continue
            genres = artist.get("genres", [])
            results[artist["id"]] = genres[0] if genres else ""
        time.sleep(SLEEP_SEC)
    return results
 
 
def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
 
# ── Main ──────────────────────────────────────────────────────────────────────
 
def main():
    # 1. Read input CSV
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Loaded {len(rows)} tracks from {INPUT_FILE}\n")
 
    # 2. Search Spotify for each row
    matched   = []   # (original_row, spotify_match_dict)
    unmatched = []   # original rows where no good match was found
 
    for i, row in enumerate(rows, 1):
        track_name = row.get("track_name", "").strip()
        artist     = row.get("artists", "").strip()
        print(f"[{i}/{len(rows)}] Searching: '{track_name}' — '{artist}'")
 
        match = search_spotify(track_name, artist)
        if match:
            print(f"    ✓ Matched (score={match['match_score']}): "
                  f"'{match['track_name']}' — '{match['artists']}'")
            matched.append((row, match))
        else:
            print(f"    ✗ No match found")
            unmatched.append(row)
 
        time.sleep(SLEEP_SEC)
 
    print(f"\nMatched {len(matched)} / {len(rows)} tracks "
          f"({len(unmatched)} unmatched)\n")
 
    if not matched:
        print("No matches — nothing to write.")
        return
 
    # 3. Batch-fetch audio features for all matched Spotify IDs
    spotify_ids = [m["spotify_id"] for _, m in matched]
    print("Fetching audio features...")
    audio_data = fetch_audio_features(spotify_ids)
    print(f"  -> Got features for {len(audio_data)} tracks")
 
    # 4. Batch-fetch artist genres
    all_artist_ids = list({
        aid
        for _, m in matched
        for aid in m["artist_ids"]
    })
    print("Fetching artist genres...")
    artist_genre_map = fetch_artist_genres(all_artist_ids)
    print(f"  -> Got genres for {len(artist_genre_map)} artists")
 
    # 5. Build enriched rows
    enriched_rows = []
    for row, match in matched:
        sid   = match["spotify_id"]
        feats = audio_data.get(sid, {})
        genre = artist_genre_map.get(match["artist_ids"][0], "") \
                if match["artist_ids"] else ""
 
        enriched_rows.append({
            "track_id":         row["track_id"],       # keep original internal ID
            "artists":          match["artists"],
            "album_name":       match["album_name"],
            "track_name":       match["track_name"],
            # "popularity":       match["popularity"],
            "duration_ms":      match["duration_ms"],
            "explicit":         match["explicit"],
            "danceability":     feats.get("danceability", ""),
            "energy":           feats.get("energy", ""),
            "key":              feats.get("key", ""),
            "loudness":         feats.get("loudness", ""),
            "mode":             feats.get("mode", ""),
            "speechiness":      feats.get("speechiness", ""),
            "acousticness":     feats.get("acousticness", ""),
            "instrumentalness": feats.get("instrumentalness", ""),
            "liveness":         feats.get("liveness", ""),
            "valence":          feats.get("valence", ""),
            "tempo":            feats.get("tempo", ""),
            "time_signature":   feats.get("time_signature", ""),
            "track_genre":      genre,
        })
 
    # 6. Write enriched output
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        writer.writerows(enriched_rows)
    print(f"\n✓ Enriched CSV  -> {OUTPUT_FILE} ({len(enriched_rows)} rows)")
 
    # 7. Write unmatched rows for manual review
    if unmatched:
        with open(UNMATCHED_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=unmatched[0].keys())
            writer.writeheader()
            writer.writerows(unmatched)
        print(f"  Unmatched CSV -> {UNMATCHED_FILE} ({len(unmatched)} rows)")
 
 
if __name__ == "__main__":
    main()