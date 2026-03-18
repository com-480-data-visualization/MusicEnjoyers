import csv
import os
from pathlib import Path
from rapidfuzz import fuzz
import unicodedata
DATASET_PATH = "source_files/dataset.csv"
global ctr_weird
ctr_weird = 0
fieldnames = [
    "billboard_id",
    "track_id",
    "Track Name",
    "Artist Name(s)",
    "Release Date",
    "Duration (ms)",
    "Explicit",
    "Genres",
    "Danceability",
    "Energy",
    "Loudness",
    "Mode",
    "Speechiness",
    "Acousticness",
    "Instrumentalness",
    "Liveness",
    "Valence",
    "Tempo",
    "Time Signature"
]
def remove_accents(text):
    return ''.join(
        c for c in unicodedata.normalize('NFKD', text)
        if not unicodedata.combining(c)
    )
def keep_letters_spaces(s):
    return "".join(c for c in s if c.isalpha() or c == " ")
def load_dataset(unmatched_file, dataset_path=DATASET_PATH ):
    """Load dataset.csv into a list of dicts for fuzzy matching."""
    records = []
    with open(dataset_path, newline="", encoding="utf-8") as f:
        
        for row in csv.DictReader(f):
            if row["track_id"].startswith("spotify:local"):
                row["track_id"] = (14 + len("5dae01pKNjRQtgOeAkFzPY")) * "0"
                # global ctr_weird
                # ctr_weird += 1
                # with open(unmatched_file, "a", newline="", encoding="utf-8") as f:
                #     #track_id,Track Name,Artist Name(s),Release Date,Duration (ms),Explicit,Genres,Danceability,Energy,Loudness,Mode,Speechiness,Acousticness,Instrumentalness,Liveness,Valence,Tempo,Time Signature

                #     csv.DictWriter(f, fieldnames=["billboard_id", "artists", "track_name"]).writerow(

                #         {"billboard_id": "0000-00-00", "artists": row["Artist Name(s)"], "track_name": row["Track Name"]}
                #     )
                # continue

            # print(row.keys())
            assert(len(row["track_id"][14:]) == len("5dae01pKNjRQtgOeAkFzPY")), (len(row["track_id"][14:]),len("5dae01pKNjRQtgOeAkFzPY"), row["track_id"])
            # assert(not row["track_id"].startswith(":"))
            
            records.append({
                "track_id": row["track_id"][14:],
                "Track Name": remove_accents(row["Track Name"].lower().strip()),
                "Artist Name(s)": remove_accents(row["Artist Name(s)"].lower().strip()),
                "Release Date": row["Release Date"],
                "Duration (ms)": row["Duration (ms)"],
                "Explicit": row["Explicit"],
                "Genres": row["Genres"],
                "Danceability": row["Danceability"],
                "Energy": row["Energy"],
                "Loudness": row["Loudness"],
                "Mode": row["Mode"],
                "Speechiness": row["Speechiness"],
                "Acousticness": row["Acousticness"],
                "Instrumentalness": row["Instrumentalness"],
                "Liveness": row["Liveness"],
                "Valence": row["Valence"],
                "Tempo": row["Tempo"],
                "Time Signature": row["Time Signature"]
                })
    return records



def match_artists(artists_a, artists_b):
    """
    Score two artist lists against each other.
    Each artist in A is matched to its best counterpart in B, and averaged.
    """
    if not artists_a or not artists_b:
        return 0
    scores = [
        max(fuzz.ratio(a, b) for b in artists_b)
        for a in artists_a
    ]
    return sum(scores) / len(scores)
def find_best_match(artist, track_name, dataset, threshold=85):
    tn = track_name.lower().strip()

    best_score, best_id = 0, None

    for record in dataset:
        if track_name in record["Track Name"]:
            continue
        # if track_name == "Like I'm Gonna Lose You".lower().strip():
        #     print("Like I'm Gonna Lose You")
        #     print(record)
        tst = artist.split(" ")[0].lower()
        # print(tst)
        # print(record['artists'])
        
        if tst in record["Artist Name(s)"].lower():
            return record
    return best_id if best_score >= threshold else None


def load_existing_ids(output_file):
    """Return the set of billboard_ids already written to the output CSV."""
    processed = set()
    if os.path.exists(output_file):
        with open(output_file, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                processed.add(row["billboard_id"])
    return processed


def fetch_track_ids(
    input_file,
    output_file="track_ids.csv",
    unmatched_file="unmatched.csv",
    dataset_path=DATASET_PATH,
    threshold=85,
):
    """
    Match billboard tracks against a local dataset CSV and write track IDs.

    Args:
        input_file:     Path to billboard CSV  (columns: billboard_id, artists, track_name)
        output_file:    Path to output CSV     (columns: billboard_id, track_id)
        unmatched_file: Path to unmatched CSV  (columns: billboard_id, artists, track_name)
        dataset_path:   Path to dataset.csv    (columns: track_id, artists, track_name)
        threshold:      Minimum fuzzy score (0-100) to count as a match (default: 85)
    """
    with open(unmatched_file, "w", newline="", encoding="utf-8") as f:
        csv.DictWriter(f, fieldnames=["billboard_id", "artists", "track_name"]).writeheader()

    dataset = load_dataset(unmatched_file, dataset_path )
    # print(f"Loaded {len(dataset)} tracks from dataset.")

    # Write headers if files don't exist yet
    if not os.path.exists(output_file):
        with open(output_file, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fieldnames).writeheader()



    # Resume support — skip already-processed IDs
    processed_ids = load_existing_ids(output_file)
    # print(f"Resuming — {len(processed_ids)} tracks already processed.")

    with open(input_file, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    pending = [r for r in rows if r["track_id"] not in processed_ids]
    # print(f"{len(pending)} tracks remaining.\n")

    matched_count, unmatched_count = 0, 0

    for i, row in enumerate(pending):
        billboard_id = row["track_id"]
        artist       = row["artists"]
        track_name   = row["track_name"]
        track = find_best_match(artist, track_name, dataset, threshold)


        if track:
            # print(track.keys())



            with open(output_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                to_add = {
                    "billboard_id": billboard_id,
                    "track_id": track["track_id"],
                    "Track Name": track_name,
                    "Artist Name(s)": artist,
                    "Release Date": track["Release Date"],
                    "Duration (ms)": track["Duration (ms)"],
                    "Explicit": track["Explicit"],
                    "Genres": track["Genres"],
                    "Danceability": track["Danceability"],
                    "Energy": track["Energy"],
                    "Loudness": track["Loudness"],
                    "Mode": track["Mode"],
                    "Speechiness": track["Speechiness"],
                    "Acousticness": track["Acousticness"],
                    "Instrumentalness": track["Instrumentalness"],
                    "Liveness": track["Liveness"],
                    "Valence": track["Valence"],
                    "Tempo": track["Tempo"],
                    "Time Signature": track["Time Signature"]
                }
                # print(to_add)
                writer.writerow(to_add)
            matched_count += 1
            print(f"[{i+1}/{len(pending)}] ✓ {billboard_id} → {track["track_id"]}")
        else:
            with open(unmatched_file, "a", newline="", encoding="utf-8") as f:
                #track_id,Track Name,Artist Name(s),Release Date,Duration (ms),Explicit,Genres,Danceability,Energy,Loudness,Mode,Speechiness,Acousticness,Instrumentalness,Liveness,Valence,Tempo,Time Signature

                csv.DictWriter(f, fieldnames=["billboard_id", "artists", "track_name"]).writerow(

                    {"billboard_id": billboard_id, "artists": artist, "track_name": track_name}
                )
            unmatched_count += 1
            # print(f"[{i+1}/{len(pending)}] ✗ {billboard_id} → no match  ({artist} – {track_name})")

    print(f"Done. Matched: {matched_count} | Unmatched: {unmatched_count}")

print(ctr_weird)
def remove_duplicate_tracks(input_csv, output_csv):
    seen_tracks = set()

    with open(input_csv, newline="", encoding="utf-8-sig") as infile, \
         open(output_csv, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            track_id = row["track_id"]

            if track_id not in seen_tracks:
                seen_tracks.add(track_id)
                writer.writerow(row)
def remove_duplicate_tracks2(input_csv, output_csv, fieldnames=fieldnames):
    seen_tracks = set()

    with open(input_csv, newline="", encoding="utf-8-sig") as infile, \
         open(output_csv, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=["billboard_id", "artists", "track_name"])
        writer.writeheader()

        for row in reader:
            track_name  = row["track_name"]
            artists     = row["artists"]
            t           = (track_name, artists)

            if t not in seen_tracks:
                seen_tracks.add(t)
                writer.writerow(row)
for i in [2016, 2017, 2018,2019,2020,2021,2022, 2023,2024,2025]:
# for i in [2016]:
    # for i in [2016]:
    print(f"year: {i}")
    fetch_track_ids(
        input_file      = f'source_files/billboard_data/enriched/billboard_hot100_{i}_monthly_enriched.csv',
        output_file     = f"results/track_ids/track_ids_{i}.csv",
        unmatched_file  = f"results/unmatched/unmatch_{i}.csv",
        dataset_path    = "source_files/Billboard_Top_100_songs_of_each_year_1950-2025.csv"
    )
for i in [2016, 2017, 2018,2019,2020,2021,2022, 2023,2024,2025]:
    remove_duplicate_tracks(
        input_csv       = f"results/track_ids/track_ids_{i}.csv",
        output_csv      = f"results/track_nodupe/track_ids_nd_{i}.csv",
    )
    remove_duplicate_tracks2(
        input_csv       = f"results/unmatched/unmatch_{i}.csv",
        output_csv      = f"results/unmatched/unmatch_{i}_nd.csv",

    )