import csv
import json
import os
import time
import urllib.parse
import urllib.request
import glob

# Years to process
YEARS = list(range(2016, 2025))

# Pick first Saturday of each month (Billboard charts are dated Saturdays)
# We'll compute approximate dates
MONTH_DATES = {
    2016: ["2016-01-02", "2016-02-06", "2016-03-05", "2016-04-02", "2016-05-07", "2016-06-04",
           "2016-07-02", "2016-08-06", "2016-09-03", "2016-10-01", "2016-11-05", "2016-12-03"],
    2017: ["2017-01-07", "2017-02-04", "2017-03-04", "2017-04-01", "2017-05-06", "2017-06-03",
           "2017-07-01", "2017-08-05", "2017-09-02", "2017-10-07", "2017-11-04", "2017-12-02"],
    2018: ["2018-01-06", "2018-02-03", "2018-03-03", "2018-04-07", "2018-05-05", "2018-06-02",
           "2018-07-07", "2018-08-04", "2018-09-01", "2018-10-06", "2018-11-03", "2018-12-01"],
    2019: ["2019-01-05", "2019-02-02", "2019-03-02", "2019-04-06", "2019-05-04", "2019-06-01",
           "2019-07-06", "2019-08-03", "2019-09-07", "2019-10-05", "2019-11-02", "2019-12-07"],
    2020: ["2020-01-04", "2020-02-01", "2020-03-07", "2020-04-04", "2020-05-02", "2020-06-06",
           "2020-07-04", "2020-08-01", "2020-09-05", "2020-10-03", "2020-11-07", "2020-12-05"],
    2021: ["2021-01-02", "2021-02-06", "2021-03-06", "2021-04-03", "2021-05-01", "2021-06-05",
           "2021-07-03", "2021-08-07", "2021-09-04", "2021-10-02", "2021-11-06", "2021-12-04"],
    2022: ["2022-01-01", "2022-02-05", "2022-03-05", "2022-04-02", "2022-05-07", "2022-06-04",
           "2022-07-02", "2022-08-06", "2022-09-03", "2022-10-01", "2022-11-05", "2022-12-03"],
    2023: ["2023-01-07", "2023-02-04", "2023-03-04", "2023-04-01", "2023-05-06", "2023-06-03",
           "2023-07-01", "2023-08-05", "2023-09-02", "2023-10-07", "2023-11-04", "2023-12-02"],
    2024: ["2024-01-06", "2024-02-03", "2024-03-02", "2024-04-06", "2024-05-04", "2024-06-01",
           "2024-07-06", "2024-08-03", "2024-09-07", "2024-10-05", "2024-11-02", "2024-12-07"],
}

FIELDS = ["track_id", "artists", "album_name", "track_name", "duration_ms", "explicit"]


def fetch_billboard(year, dates):
    """Download Billboard JSON files for a year, return list of rows."""
    data_dir = f"billboard_data_{year}"
    os.makedirs(data_dir, exist_ok=True)

    rows = []
    for date_str in dates:
        mm = date_str[5:7]
        fpath = os.path.join(data_dir, f"{date_str}.json")

        # Download if not cached
        if not os.path.exists(fpath):
            url = f"https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/date/{date_str}.json"
            print(f"  Downloading {url} ...")
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "BillboardEnricher/1.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    content = resp.read()
                with open(fpath, "wb") as f:
                    f.write(content)
            except Exception as e:
                print(f"  ERROR downloading {date_str}: {e}")
                continue

        with open(fpath) as f:
            data = json.load(f)

        chart = data.get("data", data) if isinstance(data, dict) else data
        if isinstance(chart, dict):
            chart = list(chart.values())[0] if not isinstance(list(chart.values())[0], str) else []

        for entry in chart:
            rank = entry["this_week"]
            track_id = f"{year}-{mm}-{rank:03d}"
            rows.append({
                "track_id": track_id,
                "artists": entry["artist"],
                "album_name": "",
                "track_name": entry["song"],
                "duration_ms": "",
                "explicit": "",
            })

    return rows


def deezer_search(artist, track):
    """Search Deezer for a track and return metadata."""
    first_artist = artist.split(";")[0].split(" Featuring ")[0].split(" & ")[0].split(" X ")[0].split(" With ")[0].strip()
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
                "duration_ms": t["duration"] * 1000,
                "explicit": str(t["explicit_lyrics"]).lower(),
            }
    except Exception as e:
        print(f"  Error: {e}")
    return None


def enrich_and_save(year, rows):
    """Enrich rows with Deezer metadata and save CSV."""
    output_csv = f"billboard_hot100_{year}_monthly_enriched.csv"
    total = len(rows)
    enriched = 0
    not_found = []

    for i, row in enumerate(rows):
        print(f"  [{i+1}/{total}] {row['track_name']} - {row['artists']}", end=" ... ", flush=True)
        result = deezer_search(row["artists"], row["track_name"])
        if result:
            row["album_name"] = result["album_name"]
            row["duration_ms"] = result["duration_ms"]
            row["explicit"] = result["explicit"]
            enriched += 1
            print("OK")
        else:
            not_found.append(f"    {row['track_id']}: {row['track_name']} - {row['artists']}")
            print("NOT FOUND")

        # Deezer rate limit: 50 requests per 5 seconds
        if (i + 1) % 45 == 0:
            time.sleep(5)

    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  {year} Done! Enriched: {enriched}/{total}, Not found: {len(not_found)}")
    if not_found:
        print("  Missing tracks:")
        for t in not_found:
            print(t)
    print(f"  Output: {output_csv}\n")


def main():
    for year in YEARS:
        print(f"{'='*60}")
        print(f"Processing {year}...")
        print(f"{'='*60}")

        dates = MONTH_DATES[year]
        rows = fetch_billboard(year, dates)
        print(f"  Fetched {len(rows)} chart entries")

        if rows:
            enrich_and_save(year, rows)


if __name__ == "__main__":
    main()
