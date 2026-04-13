
# Creates Figure 2: GP practice closures between 2013 and 2023
# Each red dot represents one GP practice that closed.
#
# Data source: NHS England GP and GP Practice Related Data
# https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data

import os
import pandas as pd
import plotly.express as px

DATA_LOC = '../data/gp_closures_coords.csv'
OUTPUT_DIR = '../final_output_for_article/'
OUTPUT_NAME = 'fig2_GP_closures_map.png'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

df = pd.read_csv(DATA_LOC)

fig = px.scatter_mapbox(
    df,
    lat='lat',
    lon='long',
    zoom=5.2,
    center={'lat': 52.8, 'lon': -1.8},
    mapbox_style='open-street-map',
)

fig.update_traces(
    marker=dict(size=5, color='red', opacity=0.8)
)

fig.update_layout(
    width=700,
    height=900,
    margin=dict(l=0, r=0, t=0, b=0),
    showlegend=False,
)

fig.write_image(OUTPUT_DIR + OUTPUT_NAME, scale=2)
print(f"Saved {OUTPUT_DIR + OUTPUT_NAME}")
