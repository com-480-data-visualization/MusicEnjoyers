import csv
import time
import urllib.parse
import urllib.request
import json

#INPUT_CSV = "billboard_hot100_2025_monthly.csv"
#OUTPUT_CSV = "billboard_hot100_2025_monthly_enriched.csv"
INPUT_CSV = "billboard_hot100_2015_monthly.csv"
OUTPUT_CSV = "billboard_hot100_2015_monthly_enriched.csv"
FIELDS = ["track_id", "artists", "album_name", "track_name", "duration_ms", "explicit"]


def deezer_search(artist: str, track: str):
    """Search Deezer for a track and return metadata."""
    # Try structured search first
    first_artist = artist.split(";")[0].split(" Featuring ")[0].split(" & ")[0].split(" X ")[0].strip()
    query = f'track:"{track}" artist:"{first_artist}"'
    encoded = urllib.parse.urlencode({"q": query, "limit": 3})
    url = f"https://api.deezer.com/search?{encoded}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BillboardEnricher/1.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())

        items = data.get("data", [])
        if not items:
            # Fallback: simpler search
            query = f"{track} {first_artist}"
            encoded = urllib.parse.urlencode({"q": query, "limit": 3})
            url = f"https://api.deezer.com/search?{encoded}"
            req = urllib.request.Request(url, headers={"User-Agent": "BillboardEnricher/1.0"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode())
            items = data.get("data", [])

        if items:
            t = items[0]
            return {
                "album_name": t["album"]["title"],
                "duration_ms": t["duration"] * 1000,  # Deezer returns seconds
                "explicit": str(t["explicit_lyrics"]).lower(),
                "artists": t["artist"]["name"],
            }
    except Exception as e:
        print(f"  Error: {e}")
    return None


def main():
    with open(INPUT_CSV, newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    total = len(rows)
    enriched = 0
    not_found = []

    for i, row in enumerate(rows):
        print(f"[{i+1}/{total}] {row['track_name']} - {row['artists']}", end=" ... ", flush=True)
        result = deezer_search(row["artists"], row["track_name"])
        if result:
            row["album_name"] = result["album_name"]
            row["duration_ms"] = result["duration_ms"]
            row["explicit"] = result["explicit"]
            # Keep Billboard artist names (more complete with features)
            enriched += 1
            print("OK")
        else:
            not_found.append(f"  {row['track_id']}: {row['track_name']} - {row['artists']}")
            print("NOT FOUND")

        # Deezer rate limit: 50 requests per 5 seconds
        if (i + 1) % 45 == 0:
            time.sleep(5)

    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone! Enriched: {enriched}/{total}, Not found: {len(not_found)}")
    if not_found:
        print("Missing tracks:")
        for t in not_found:
            print(t)
    print(f"Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
