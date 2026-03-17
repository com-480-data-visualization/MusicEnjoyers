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
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from creds import CLIENT_SECRET, CLIENT_ID
import time
from spotipy.exceptions import SpotifyException
from tqdm import tqdm

INPUT_FILE = "./source_files/billboard_configured.csv"
def query_and_store_track_id(
    song_name: str,
    artist: str,
    billboard_id: str,
    filename="track_ids.csv",
    max_retries=5
):
    """
    Query Spotify for a track_id and store it with billboard_id.
    Prevents duplicate billboard_id entries.
    Handles 429 rate-limit errors using exponential backoff.
    """

    auth_manager = SpotifyClientCredentials(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    sp = spotipy.Spotify(auth_manager=auth_manager)

    # Check existing billboard_ids
    existing_billboard_ids = set()

    if os.path.isfile(filename):
        with open(filename, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            existing_billboard_ids = {row[1] for row in reader if len(row) >= 2}

    if billboard_id in existing_billboard_ids:
        print("Billboard ID already stored.")
        return None

    query = f"track:{song_name} artist:{artist}"

    attempt = 0
    backoff = 1

    while attempt < max_retries:

        try:
            results = sp.search(q=query, type="track", limit=1)
            tracks = results["tracks"]["items"]

            if not tracks:
                print("Track not found.")
                append_not_found(billboard_id=billboard_id, artist=artist, song_name=song_name)
                return None
            track_id = tracks[0]["id"]

            with open(filename, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([track_id, billboard_id])

            print(f"Stored track_id {track_id} with billboard_id {billboard_id}")
            return track_id

        except SpotifyException as e:

            if e.http_status == 429:
                retry_after = int(e.headers.get("Retry-After", backoff))
                wait_time = max(retry_after, backoff)

                print(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)

                backoff *= 2
                attempt += 1

            else:
                print(f"Spotify API error: {e}")
                return None

        except Exception as e:
            print(f"Unexpected error: {e}")
            return None

    print("Max retries exceeded.")
    return None
def append_not_found(billboard_id: str, artist: str, song_name: str, filename="not_found.csv"):
    """
    Append songs that could not be matched on Spotify.
    Only adds the row if the billboard_id is not already present.
    """

    try:
        existing_ids = set()

        if os.path.isfile(filename):
            with open(filename, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                existing_ids = {row[0] for row in reader if row}

        if billboard_id in existing_ids:
            return

        with open(filename, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([billboard_id, artist, song_name])

    except Exception as e:
        print(f"Error writing not-found entry: {e}")


def query_and_store_song_info(
    input_csv="track_ids.csv",
    output_csv="song_info.csv",
    max_retries=5,
    retry_delay=1  # 30 minutes for pending premium
):
    """
    Given a CSV with 'track_id,billboard_id', query Spotify for song info + audio features
    and append to output CSV. Skips entries whose billboard_id already exists in output CSV.
    Handles 429 rate-limits with exponential backoff.
    """

    # Initialize Spotify client once
    auth_manager = SpotifyClientCredentials(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    sp = spotipy.Spotify(auth_manager=auth_manager)

    # Track existing billboard_ids in output CSV to avoid duplicates
    existing_billboard_ids = set()
    if os.path.isfile(output_csv):
        with open(output_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            existing_billboard_ids = {row[0] for row in reader if row}

    # Ensure header exists in output CSV
    if not os.path.isfile(output_csv):
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "billboard_id", "track_id", "name", "artists", "album",
                "release_date", "duration_ms", "popularity", "explicit",
                "spotify_url", "danceability", "energy", "key", "loudness",
                "mode", "speechiness", "acousticness", "instrumentalness",
                "liveness", "valence", "tempo", "time_signature"
            ])

    # Read input CSV
    with open(input_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        i = 0
        for row in tqdm(reader):
            i += 1
            if i == 5 : break
            if len(row) < 2:
                continue
            track_id, billboard_id = row[0], row[1]

            if billboard_id in existing_billboard_ids:
                continue  # already processed

            attempt = 0
            backoff = 1

            while attempt < max_retries:
                try:
                    # Query track metadata
                    track = sp.track(track_id)
                    # print(track)
                    
                    features = sp.audio_features(track_id)[0]
                    print(features)
                    break
                    song_info = [
                        billboard_id,
                        track_id,
                        track["name"],
                        "; ".join([a["name"] for a in track["artists"]]),
                        track["album"]["name"],
                        track["album"]["release_date"],
                        track["duration_ms"],
                        track["popularity"],
                        track["explicit"],
                        track["external_urls"]["spotify"],
                        features["danceability"],
                        features["energy"],
                        features["key"],
                        features["loudness"],
                        features["mode"],
                        features["speechiness"],
                        features["acousticness"],
                        features["instrumentalness"],
                        features["liveness"],
                        features["valence"],
                        features["tempo"],
                        features["time_signature"]
                    ]

                    # Append to CSV
                    with open(output_csv, "a", newline="", encoding="utf-8") as out_f:
                        writer = csv.writer(out_f)
                        writer.writerow(song_info)

                    existing_billboard_ids.add(billboard_id)
                    print(f"Stored info for {billboard_id} ({track['name']})")
                    break  # success → exit retry loop

                except SpotifyException as e:
                    if e.http_status == 429:
                        # Rate-limited → exponential backoff
                        retry_after = int(e.headers.get("Retry-After", backoff))
                        wait_time = max(retry_after, backoff)
                        print(f"Rate limited. Waiting {wait_time}s...")
                        time.sleep(wait_time)
                        backoff *= 2
                        attempt += 1

                    # elif e.http_status == 403 and "Active premium subscription" in str(e):
                    elif e.http_status == 403:
                        # Pending premium → wait and retry later
                        print(e)
                        print(f"Track {billboard_id} requires active premium. Waiting {retry_delay}s before retry...")
                        time.sleep(retry_delay)

                    else:
                        print(f"Spotify API error for {billboard_id}: {e}")
                        break  # skip track

                except Exception as e:
                    print(f"Unexpected error for {billboard_id}: {e}")
                    break

            else:
                print(f"Max retries exceeded for {billboard_id}")
def batch_query_and_store_song_info(
    input_csv="track_ids.csv",
    output_csv="song_info.csv",
    batch_size=50,
    max_retries=5,
    retry_delay=1800  # 30 minutes for pending premium
):
    """
    Reads a CSV with track_id,billboard_id and queries Spotify in batches for song info + audio features.
    Appends results to output CSV.
    Handles duplicates, 429 rate limits (exponential backoff), and 403 pending premium errors.
    """

    # Initialize Spotify client
    sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    ))

    # Track existing billboard_ids
    existing_billboard_ids = set()
    if os.path.isfile(output_csv):
        with open(output_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            existing_billboard_ids = {row[0] for row in reader if row}

    # Ensure header exists
    if not os.path.isfile(output_csv):
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "billboard_id", "track_id", "name", "artists", "album",
                "release_date", "duration_ms", "popularity", "explicit",
                "spotify_url", "danceability", "energy", "key", "loudness",
                "mode", "speechiness", "acousticness", "instrumentalness",
                "liveness", "valence", "tempo", "time_signature"
            ])

    # Read input CSV and filter unprocessed tracks
    track_rows = []
    with open(input_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            track_id, billboard_id = row[0], row[1]
            if billboard_id not in existing_billboard_ids:
                track_rows.append((track_id, billboard_id))

    # Process in batches
    for i in range(0, len(track_rows), batch_size):
        batch = track_rows[i:i+batch_size]
        track_ids = [t[0] for t in batch]
        billboard_ids = [t[1] for t in batch]

        attempt = 0
        backoff = 1

        while attempt < max_retries:
            try:
                # Query track metadata individually (still per track)
                tracks_info = []
                features_info = sp.audio_features(track_ids)

                for idx, track_id in enumerate(track_ids):
                    try:
                        track = sp.track(track_id)
                        features = features_info[idx]
                        if not features:
                            print(f"No features for track {track_id}")
                            continue

                        song_info = [
                            billboard_ids[idx],
                            track_id,
                            track["name"],
                            "; ".join([a["name"] for a in track["artists"]]),
                            track["album"]["name"],
                            track["album"]["release_date"],
                            track["duration_ms"],
                            track["popularity"],
                            track["explicit"],
                            track["external_urls"]["spotify"],
                            features["danceability"],
                            features["energy"],
                            features["key"],
                            features["loudness"],
                            features["mode"],
                            features["speechiness"],
                            features["acousticness"],
                            features["instrumentalness"],
                            features["liveness"],
                            features["valence"],
                            features["tempo"],
                            features["time_signature"]
                        ]
                        tracks_info.append(song_info)
                        existing_billboard_ids.add(billboard_ids[idx])
                        print(f"Processed {billboard_ids[idx]} ({track['name']})")
                    except SpotifyException as e:
                        if e.http_status == 403 and "Active premium subscription" in str(e):
                            print(f"Track {billboard_ids[idx]} pending premium. Skipping for now.")
                        else:
                            print(f"Error for track {track_id}: {e}")
                    except Exception as e:
                        print(f"Unexpected error for track {track_id}: {e}")

                # Write batch to CSV
                if tracks_info:
                    with open(output_csv, "a", newline="", encoding="utf-8") as f_out:
                        writer = csv.writer(f_out)
                        writer.writerows(tracks_info)
                break  # batch processed successfully

            except SpotifyException as e:
                if e.http_status == 429:
                    retry_after = int(e.headers.get("Retry-After", backoff))
                    wait_time = max(retry_after, backoff)
                    print(f"Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    backoff *= 2
                    attempt += 1
                else:
                    print(f"Spotify API error during batch: {e}")
                    break
            except Exception as e:
                print(f"Unexpected error during batch: {e}")
                break
        else:
            print(f"Max retries exceeded for batch starting with {billboard_ids[0]}")
# with open(INPUT_FILE, newline="", encoding="utf-8") as f:
#     rows = list(csv.DictReader(f))
#     print(rows[0])
#     print(f"Loaded {len(rows)} tracks from {INPUT_FILE}")
#     for ctr in tqdm(range(len(rows))):
#         i = rows[ctr]
#         if "Featuring" in i["artists"]:
#              s = i["artists"].split(" Featuring ")[0]
#              i["artists"] = s
#         query_and_store_track_id(song_name=i["track_name"], artist=i["artists"], billboard_id=i["track_id"])
        # time.sleep(0.5)


query_and_store_song_info()
# batch_query_and_store_song_info()