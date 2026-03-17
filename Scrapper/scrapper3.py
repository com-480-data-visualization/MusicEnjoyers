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
import http.client
INPUT_FILE = "./source_files/billboard_configured"
BATCH_SIZE = 1
import json

def load_existing_ids(filename, idx = 1):
    """Load existing billboard_ids to prevent duplicates"""
    ids = set()

    if os.path.isfile(filename):
        with open(filename, "r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            ids = {row[idx] for row in reader if len(row) >= 2}

    return ids


def batch(iterable, size):
    """Yield batches"""
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def process_file(input_file, output_file="track_ids2.csv", max_retries=5):

    auth_manager = SpotifyClientCredentials(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET
    )

    sp = spotipy.Spotify(auth_manager=auth_manager)

    existing_ids = load_existing_ids(output_file)

    rows = []

    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if row["billboard_id"] not in existing_ids:
                rows.append(row)

    print(f"{len(rows)} tracks remaining to query")

    for group in tqdm(batch(rows, BATCH_SIZE), total=len(rows)//BATCH_SIZE):

        query = " OR ".join(
            f'track:"{r["track_name"]}" artist:"{r["artists"]}"'
            for r in group
        )

        attempt = 0
        backoff = 1

        while attempt < max_retries:

            try:

                results = sp.search(q=query, type="track", limit=len(group))

                tracks = results["tracks"]["items"]

                with open(output_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)

                    for r, track in zip(group, tracks):

                        if track:
                            track_id = track["id"]
                            writer.writerow([track_id, r["billboard_id"]])
                            # print("Stored:", r["billboard_id"])
                        else:
                            print("Not found:", r["billboard_id"])
                            append_not_found(billboard_id=r["billboard_id"], artist=r["artists"], track_name=r["track_name"])

                break

            except SpotifyException as e:

                if e.http_status == 429:

                    retry_after = e.headers.get("Retry-After")
                    wait = int(retry_after) if retry_after else backoff

                    print(f"Rate limited. Sleeping {wait}s")
                    time.sleep(wait)

                    backoff *= 2
                    attempt += 1

                else:
                    print("Spotify error:", e)
                    break


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


def query_and_store_song_info(input_file = './track_ids.csv', output_file = 'output.csv', bs=10):
    rows = []
    existing_ids = load_existing_ids(output_file,0)
    with open(input_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if row["billboard_id"] not in existing_ids:
                rows.append(row)
    print(len(rows))
    print(f'quering: {len(rows)} entries')
    for groups in tqdm(batch(rows[:],bs)):

        conn = http.client.HTTPSConnection("api.reccobeats.com")

        endpoint = "/v1/audio-features?ids="
        payload = ''
        for i in groups:
            payload = payload + i["track_id"] + ","
        payload = payload[:-1] #remove last ,
        
        # print(endpoint)
        # continue
        headers = {
        'Accept': 'application/json'
        }
        conn.request("GET", endpoint + payload, headers = headers)
        res1 = conn.getresponse()
        audio_features = res1.read()
        # print(audio_features)
        audio_features = json.loads(audio_features)
        # print("audio features:")
        # print(audio_features['content'][0].keys())
        # print("\ntrack_features")
        conn.request("GET", "/v1/track?ids=" + payload, headers = headers)
        res2 = conn.getresponse()
        track_features = json.loads(res2.read())
        if "error" in audio_features or "error" in track_features:
            for k in groups:
                append_not_found(billboard_id = k['billboard_id'], artist = 'NA', song_name = "NA")
            print("Response Error")
            time.sleep(3)
            continue
        if (len(audio_features['content']) != BATCH_SIZE) or (len(track_features['content']) != BATCH_SIZE):
            for k in groups:
                append_not_found(billboard_id = k['billboard_id'], artist = 'NA', song_name = "NA")
            print("response size mismatch")
            time.sleep(0.5)
            continue
        wanted_keys_audio = ['id', 'acousticness', 'danceability', \
                        'energy', 'instrumentalness', 'key', 'liveness', \
                        'loudness', 'mode', 'speechiness', 'tempo', 'valence']

        wanted_keys_track = ['id', 'trackTitle', 'artists', 'durationMs']
        track_features_relevant = []

        for c in range(BATCH_SIZE):
            # try:
                # print(c)
                tmp_audio = {k:audio_features['content'][c][k] for k in wanted_keys_audio}
                # print("ok1")
                tmp_track = {k:track_features['content'][c][k] for k in wanted_keys_track}
                # print("ok2")
                tmp_track['artists'] = " || ".join([a["name"] for a in tmp_track["artists"]])
                
                compact = {'billboard_id': groups[c]['billboard_id'],**tmp_track, **tmp_audio}
                track_features_relevant.append(compact)
                # print(json.dumps(compact, indent=2))
                fieldnames = track_features_relevant[0].keys()
                file_exists = os.path.isfile(output_file)
                with open(output_file, "a", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=track_features_relevant[0].keys())

                    # Only write header if file is new
                    if not file_exists:
                        writer.writeheader()

                    # Write rows
                    writer.writerows(track_features_relevant)
            # except Exception as e:


        time.sleep(0.5)
        # print(track_features['content'][0].keys())

# with open(INPUT_FILE, newline="", encoding="utf-8") as f:
#     rows = list(csv.DictReader(f))
#     print(rows[0])
#     print(f"Loaded {len(rows)} tracks from {INPUT_FILE}")
#     for ctr in tqdm(range(len(rows))):
#         i = rows[ctr]
#         if "Featuring" in i["artists"]:
#              s = i["artists"].split(" Featuring ")[0]
#              i["artists"] = s
#         query_and_store_track_url(song_name=i["track_name"], artist=i["artists"], billboard_id=i["track_id"])
#         # time.sleep(0.5)

process_file(input_file=INPUT_FILE + "_2024.csv",output_file=(INPUT_FILE + "_2024_output.csv"))

query_and_store_song_info(input_file= INPUT_FILE + "_2024.csv", bs=16)
query_and_store_song_info(input_file= INPUT_FILE + "_2024.csv", bs=8)
query_and_store_song_info(input_file= INPUT_FILE + "_2024.csv", bs=4)
query_and_store_song_info(input_file= INPUT_FILE + "_2024.csv", bs=1)


# query_and_store_song_info()
# batch_query_and_store_song_info()