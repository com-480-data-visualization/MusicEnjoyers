## Authors

| Name | SCIPER |
| ---- | ------ |
| Hsieh Wei-En | 341271 |
| Li An-Jie | 424517 |
| Rohner Kenji | 425036 |

## Milestones

All deliverables (PDFs) are in:

- **Milestone 1**: Proposal & EDA: [Milestone 1.pdf](./Milestone%201.pdf)
- **Milestone 2**: Functional prototype: [Milestone2_MusicEnjoyers.pdf](./Milestone2_MusicEnjoyers.pdf)
- **Milestone 3**: Final project: [Milestone3_Processbook.pdf](./Milestone3_Processbook.pdf)


## Screencast

A screencast of the project is included in the repository: [Screencast.mp4](./Screencast.mp4).


# Technical Overview

## Folder Structure

```
├── basic_statistics.ipynb        Exploratory data analysis (Milestone 1)
├── Scrapper/                     Data collection
│   ├── scrapper.py               Matches Billboard songs to Spotify track data (fuzzy matching)
│   ├── source_files/             Billboard Hot 100 source data
│   └── results/                  Matched / unmatched track ids
├── scripts/                      Build JSON stats served to the website
├── scripts_billboard_related/    Billboard enrichment helpers
├── docs/                         The website (served via GitHub Pages)
│   ├── index.html                Page skeleton
│   ├── *.css                     Styles
│   ├── scripts/                  Visualization JS (such as D3)
│   └── data/                     Pre-computed JSON consumed by the visualizations
└── Milestones1_README.md         Detailed milestone write-ups & deliverables
```


## Hosting the Website Locally

The website is fully static: everything lives in `docs/`. To run it locally:

```bash
cd docs
python3 -m http.server 8000
```

Then open <http://localhost:8000> in your browser.

> **Tip:** if you changed a JS/CSS file but don't see the update, hard-refresh to bust the cache:
> `Ctrl + Shift + R` (Linux/Windows) or `Cmd + Shift + R` (macOS).

Press `Ctrl + C` to stop the server. If port 8000 is stuck in use:

```bash
lsof -i :8000
kill <PID>
```

# Workflow

Our Final workflow was as follows:

1. Scrap data from Spotify
2. Extract the relevant statistics to JSON
3. Create the HTML Skeleton for the website
4. Create the JS scripts for the visualization


### Scrap the data from spotify

All relevant scripts are in the folder "Scrapper". The core script is written in python and it connects the spotify data stored in source_files/Billboard_Top_100_songs_of_each_year_1950-2025.csv and connects it to the yearly files in source_files/billboard_data.

While generating the data, the script will ignore already matched songs and only look at unmatched songs. We used this feature to run the script repeatedly while adjusting the strictness of the fuzzy matching.

Due to API limits and getting blocked in the end repeatedly for 24 hrs, we had to resort to using Exportify on the following playlist: https://open.spotify.com/playlist/3aTYOIaiU9lsysHRCMppEU

### Extract the relevant statistics to JSON

In order to serve this data to a website, we extracted the information we want to display in seperate scripts in ./scripts/ and made severable json files stored in ./docs/data. This way the website does not have to do any real computation when loading the page.

### Create the Website

All files used by our website are in ./docs. It follows a simple layout, where the html and css file are in the first level and all our JS-scripts are in ./docs/scripts and the data used by those scripts are in ./docs/data.