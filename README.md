# 🎵 Pop Music Evolution 🎵

Project for the COM-480 Data Visualization course at EPFL.

> **Songs are getting shorter: about 13 seconds shorter on average over the last decade. And it is not a coincidence.**
> **Genres are shifting.** Come interact with the data.

<p align="center">
    <a href="https://com-480-data-visualization.github.io/MusicEnjoyers/">▶️ Go to the website</a>
</p>

## 🔴 Abstract

Music listeners today pursue immediate satisfaction. Song intros are shorter than they used to be, artists race to the catchy hook before listeners hit "skip", and short-video platforms reward the most viral few seconds of a track. At the same time, scholars disagree on whether digital platforms are pushing popular music toward *convergence* (everything sounds alike) or *fragmentation* (endless niche genres).

This project turns those questions into an interactive, data-driven story about how pop music has evolved over the past decade. Using the Billboard Hot 100 enriched with Spotify/Deezer audio features, we visualize how the average song has shrunk by more than 10 seconds, how genres have shifted and clustered, and how musical features (energy, danceability, valence, tempo…) relate to one another: so you can explore the trends yourself.

## 👨‍👩‍👧 Target audience

Anyone curious about music and how it has changed: casual listeners who want to *see* why songs feel shorter today, and data-minded readers who want to dig into genre dynamics and audio features.

## 🚀 Project structure

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
└── Milestones_README.md          Detailed milestone write-ups & deliverables
```

## 💻 Running the website locally

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

## 💿 Dataset

We build on two main sources:

- **[Billboard Hot 100](https://github.com/mhollingshead/billboard-hot-100)**: a widely-used measure of mainstream popularity, ranking songs by streaming, radio play, and sales. It identifies *which* songs were popular and *when*.
- **Spotify / Deezer audio features**: release date, duration, genres, and audio attributes (energy, danceability, loudness, valence, tempo, …) used to characterize *how* the music sounds.

Our processed dataset covers Billboard Hot 100 songs over the last decade (2016–2025), each with 16 attributes.

## ⚙️ Technical overview

Our workflow:

1. **Scrape the data.** `Scrapper/scrapper.py` connects the Billboard list (`source_files/Billboard_Top_100_songs_of_each_year_1950-2025.csv`) to the yearly files in `source_files/billboard_data` and enriches each song with Spotify track data. The script skips already-matched songs and only processes unmatched ones, so we could run it repeatedly while loosening the fuzzy-matching strictness.
2. **Extract statistics to JSON.** The scripts in `scripts/` (and `scripts_billboard_related/`) pre-compute everything the page needs and write JSON files into `docs/data/`. This keeps the website fast: no heavy computation happens in the browser.
3. **Build the website.** `docs/index.html` provides the skeleton; the CSS files style it; and the visualizations in `docs/scripts/` (`dashboard.js`, `genres.js`, `heatmap.js`, `duration.js`, `dj_knob.js`, `scroll.js`) read from `docs/data/` to render the interactive charts.

## 📍 Milestones

Detailed write-ups, the exploratory data analysis, and all deliverables (PDFs) are in **[Milestones_README.md](Milestones_README.md)**.

- **Milestone 1**: Proposal & EDA: [Milestone 1.pdf](./Milestone%201.pdf)
- **Milestone 2**: Functional prototype: [Milestone2_MusicEnjoyers.pdf](./Milestone2_MusicEnjoyers.pdf)
- **Milestone 3**: Final project: [Milestone 3.pdf](./Milestone%203.pdf)

## 📽 Screencast

A screencast of the project is included in the repository: [datavis_cut.mp4](./datavis_cut.mp4).

## 🤝 Authors

| Name | SCIPER |
| ---- | ------ |
| Hsieh Wei-En | 341271 |
| Li An-Jie | 424517 |
| Rohner Kenji | 425036 |
</content>
</invoke>
