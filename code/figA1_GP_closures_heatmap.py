
# Creates Figure A1: GP Practice Closures heatmap
# Density heatmap showing the geographic concentration of GP practice closures
# in England between 2013 and 2022.
#
# Data source: NHS England GP and GP Practice Related Data
# https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data

import os
import pandas as pd
import plotly.express as px

DATA_LOC   = '../data/gp_closures_coords.csv'
OUTPUT_DIR = '../final_output_for_article/'
OUTPUT_NAME = 'figA1_GP_closures_heatmap.png'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

df = pd.read_csv(DATA_LOC)

fig = px.density_mapbox(
    df,
    lat='lat',
    lon='long',
    radius=18,
    zoom=5.2,
    center={'lat': 52.8, 'lon': -1.8},
    mapbox_style='open-street-map',
    color_continuous_scale='turbo',
)

fig.update_layout(
    width=700,
    height=900,
    margin=dict(l=0, r=0, t=0, b=0),
    showlegend=False,
    coloraxis_showscale=False,
)

fig.write_image(OUTPUT_DIR + OUTPUT_NAME, scale=2)
print(f"Saved {OUTPUT_DIR + OUTPUT_NAME}")
