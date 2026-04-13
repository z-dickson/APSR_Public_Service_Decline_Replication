
# Creates Figure A2: GP Practice Closures per Local Authority Population
# Choropleth map showing closures per 10,000 residents in each English local authority,
# with an inset histogram of the same distribution.
#
# Data sources:
#   - GP closures (pre-processed from NHS England epraccur data)
#   - 2021 LAD population estimates: ONS (https://www.ons.gov.uk/explore-local-statistics/indicators/population-count)
#   - Local Authority District boundaries (May 2024, Ultra-Generalised):
#     Download GeoJSON from ONS Open Geography Portal:
#     https://geoportal.statistics.gov.uk/datasets/ons::local-authority-districts-may-2024-boundaries-uk-buc-2/about
#     Save as: ../raw_data/local_authority_districts.geojson

import os
import sys
import warnings
import requests
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import Normalize

warnings.filterwarnings('ignore')

CLOSURES_FILE  = '../data/gp_closures_coords.csv'
POPULATION_FILE = '../data/population-count-table-data.csv'
GEOJSON_FILE   = '../data/Local_Authority_Districts_May_2024_Boundaries_UK_BFE_7458506961569058424.geojson'
OUTPUT_DIR     = '../final_output_for_article/'
OUTPUT_NAME    = 'figA2_GP_closures_choropleth.png'

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)



# ── Load data ─────────────────────────────────────────────────────────────────

closures = pd.read_csv(CLOSURES_FILE)

# Count closures per local authority district
closure_counts = closures.groupby('oslaua').size().rename('closure_count')

# Population (2021 estimates)
pop = pd.read_csv(POPULATION_FILE, encoding='latin1', skiprows=7)
pop_2021 = pop.set_index('Area code')['2021'].to_dict()

# LAD boundaries — England only
gdf = gpd.read_file(GEOJSON_FILE)
gdf = gdf.rename(columns={'LAD24CD': 'lad_code', 'LAD24NM': 'lad_name'})
gdf = gdf[gdf['lad_code'].str.startswith('E', na=False)].copy()

# Attach closure counts and population
gdf['closure_count'] = gdf['lad_code'].map(closure_counts).fillna(0)
gdf['population_2021'] = gdf['lad_code'].map(pop_2021)
gdf['closures_per_10k'] = gdf['closure_count'] / gdf['population_2021'] * 10_000
gdf = gdf.dropna(subset=['closures_per_10k'])

# Project to British National Grid for accurate display
gdf = gdf.to_crs(27700)


# ── Plot ──────────────────────────────────────────────────────────────────────

VALUE_COL  = 'closures_per_10k'
CMAP       = 'bwr'
EDGE_COLOR = '#FFFFFF'
EDGE_WIDTH = 0.15

vmin = gdf[VALUE_COL].min()
vmax = gdf[VALUE_COL].max()
norm = Normalize(vmin=vmin, vmax=vmax)
cmap_obj = plt.cm.get_cmap(CMAP)

fig, ax = plt.subplots(figsize=(5.2, 6.2), dpi=300)

xmin, ymin, xmax, ymax = gdf.total_bounds
pad = 0.02
ax.set_xlim(xmin - pad * (xmax - xmin), xmax + pad * (xmax - xmin))
ax.set_ylim(ymin - pad * (ymax - ymin), ymax + pad * (ymax - ymin))

gdf.plot(ax=ax, column=VALUE_COL, cmap=CMAP, linewidth=EDGE_WIDTH,
         edgecolor=EDGE_COLOR, norm=norm)

ax.set_axis_off()

# Inset histogram colored by the choropleth colormap
hist_ax = ax.inset_axes([0, 0.62, 0.30, 0.22], transform=ax.transAxes)
values = gdf[VALUE_COL].values
counts, bin_edges, patches = hist_ax.hist(values, bins=12, orientation='vertical', density=True)
for centre, patch in zip(0.5 * (bin_edges[:-1] + bin_edges[1:]), patches):
    patch.set_facecolor(cmap_obj(norm(centre)))

hist_ax.set_xlabel('Closures/10,000 People', fontsize=7)
hist_ax.set_ylabel('Density', fontsize=7)
hist_ax.tick_params(axis='both', labelsize=7)
hist_ax.spines['top'].set_visible(False)
hist_ax.spines['right'].set_visible(False)

plt.subplots_adjust(left=0.01, right=0.99, top=0.94, bottom=0.02)
fig.savefig(OUTPUT_DIR + OUTPUT_NAME, bbox_inches='tight')
plt.close()
print(f"Saved {OUTPUT_DIR + OUTPUT_NAME}")
