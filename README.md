
# Technical Overview

Our Final workflow was as follows:

1. Scrap data from Spotify
2. Extract the relevant statistics to JSON
3. Create the HTML Skeleton for the website
4. Create the JS scripts for the visualization


## Scrap the data from spotify

All relevant scripts are in the folder "Scrapper". The core script is written in python and it connects the spotify data stored in source_files/Billboard_Top_100_songs_of_each_year_1950-2025.csv and connects it to the yearly files in source_files/billboard_data.

While generating the data, the script will ignore already matched songs and only look at unmatched songs. We used this feature to run the script repeatedly while adjusting the strictness of the fuzzy matching.

## Extract the relevant statistics to JSON

In order to serve this data to a website, we extracted the information we want to display in seperate scripts in ./scripts/ and made severable json files stored in ./docs/data. This way the website does not have to do any real computation when loading the page.

## Create the Website


All files used by our website are in ./docs. It follows a simple layout, where the html and css file are in the first level and all our JS-scripts are in ./docs/scripts and the data used by those scripts are in ./docs/data.

The java script in particular: