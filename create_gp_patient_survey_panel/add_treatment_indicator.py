"""
Add treatment indicators and group variables to the GP Patient Survey panel.

New columns added:
  treated              – 1 from the year a practice closes onwards (0 otherwise)
  treated_nearest      – 1 from the year the nearest GP practice closed onwards
  gvar                 – year of first closure in the practice's MSOA21 (10000 if none)
  gvar_nearest         – year the nearest closed GP practice closed (10000 if none)
  dist_nearest_closed  – Euclidean distance (degrees) to the nearest closed GP
                         practice within the panel window (NaN if no closed practice
                         found); suitable for use as a continuous covariate

Both gvar variables are restricted to closures within the panel window (2012–2023).
Nearest-practice search uses Euclidean distance on lat/long coordinates.
Closure data comes from get_practice_closures.gp_closures(), which filters
epraccur to GP practices only (Prescribing setting == 4).

Input:
  data/GP patient survey/gp_panel.csv
  data/epraccur/epraccur.csv         (via get_practice_closures.gp_closures())
  data/postcodes_2023.parquet        (via get_practice_closures.gp_closures())

Output:
  data/GP patient survey/gp_panel.csv   (panel with new columns prepended)
"""

import os
import sys
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

CODE_DIR   = os.path.dirname(os.path.abspath(__file__))
BASE       = os.path.join(CODE_DIR, "..", "data")
PANEL_PATH = os.path.join(CODE_DIR, "GP patient survey", "gp_panel.csv")
PANEL_YEARS = (2012, 2023)


# ── Build GP closures via get_practice_closures.py ────────────────────────────
# That module uses relative paths assuming cwd == code/, so we temporarily
# change directory before importing.
_orig_dir = os.getcwd()
os.chdir(CODE_DIR)
sys.path.insert(0, CODE_DIR)
from get_practice_closures import gp_closures
gp = gp_closures().to_pandas()
os.chdir(_orig_dir)

# lat/long are already float64; ensure msoa21 nulls are real NaN
gp["msoa21"] = gp["msoa21"].replace("nan", np.nan)
gp["close_year"] = gp["close_date"].dt.year.astype("Int64")   # nullable int

# One row per practice, earliest close date wins
closed_all = (
    gp[gp["close_date"].notna()]
    .sort_values("close_year")
    .drop_duplicates("organisation_code", keep="first")
    .copy()
)
closed_panel = closed_all[
    closed_all["close_year"].between(*PANEL_YEARS)
].copy()

print(f"GP practices total        : {len(gp):,}")
print(f"GP practices closed       : {len(closed_all):,}")
print(f"  within {PANEL_YEARS[0]}–{PANEL_YEARS[1]} : {len(closed_panel):,}")


# ── Practice geography lookup ─────────────────────────────────────────────────
# gp_closures() covers all GP practices (open + closed) so it gives us
# lat, long, msoa21 for every practice in the panel.
practice_geo = (
    gp[["organisation_code", "lat", "long", "msoa21"]]
    .drop_duplicates("organisation_code")
    .set_index("organisation_code")
)


# ── Load panel ────────────────────────────────────────────────────────────────
print("\nLoading panel …")
panel = pd.read_csv(PANEL_PATH, dtype=str, low_memory=False)
panel["year"] = panel["year"].astype(int)

# Attach geography to every panel row
panel["lat"]    = panel["practice_code"].map(practice_geo["lat"])
panel["long"]   = panel["practice_code"].map(practice_geo["long"])
panel["msoa21"] = panel["practice_code"].map(practice_geo["msoa21"])


# ── 1. treated: direct closure indicator ─────────────────────────────────────
close_year_map = closed_all.set_index("organisation_code")["close_year"].astype(float)
panel["_cy"] = panel["practice_code"].map(close_year_map)
panel["treated"] = (panel["_cy"].notna() & (panel["year"] >= panel["_cy"])).astype(int)
panel = panel.drop(columns=["_cy"])


# ── 2. treated_nearest: nearest-practice indicator ────────────────────────────
# Build KD-tree over the unique panel practices that have valid coordinates.
panel_unique = (
    practice_geo.loc[
        practice_geo.index.isin(panel["practice_code"].unique())
        & practice_geo["lat"].notna()
        & practice_geo["long"].notna()
    ]
    .copy()
)
panel_coords = panel_unique[["lat", "long"]].values
panel_codes  = panel_unique.index.values          # aligned with panel_coords

panel_tree = cKDTree(panel_coords)

# For each closed GP practice (all years), find its nearest OTHER panel practice.
# Query k=3 to safely skip the practice itself (distance ≈ 0).
closed_geo = closed_all.dropna(subset=["lat", "long"])
k = min(3, len(panel_codes))
dists, idxs = panel_tree.query(closed_geo[["lat", "long"]].values, k=k)

# nearest_map: panel_practice_code → earliest closure year for which it is nearest
nearest_map: dict[str, int] = {}
for i, row in enumerate(closed_geo.itertuples(index=False)):
    pc = row.organisation_code
    cy = int(row.close_year)
    for j in range(k):
        candidate = panel_codes[idxs[i, j]]
        if candidate != pc:
            prev = nearest_map.get(candidate)
            nearest_map[candidate] = cy if prev is None else min(prev, cy)
            break

panel["_nty"] = panel["practice_code"].map(nearest_map)
panel["treated_nearest"] = (
    panel["_nty"].notna() & (panel["year"] >= panel["_nty"])
).astype(int)
panel = panel.drop(columns=["_nty"])


# ── 3. gvar: MSOA21 first-closure year ───────────────────────────────────────
# For each MSOA21, the earliest closure year within the panel window.
msoa_first = (
    closed_panel.dropna(subset=["msoa21"])
    .groupby("msoa21")["close_year"]
    .min()
    .astype(int)
)
panel["gvar"] = panel["msoa21"].map(msoa_first).fillna(10000).astype(int)


# ── 4. gvar_nearest: year of nearest closed practice (panel window) ───────────
# For each panel practice, find the nearest closed practice (within 2012–2023)
# and record that closure's year. Closed practices exclude themselves.
closed_panel_geo = closed_panel.dropna(subset=["lat", "long"])
closed_coords = closed_panel_geo[["lat", "long"]].values
closed_years  = closed_panel_geo["close_year"].astype(int).values
closed_codes  = closed_panel_geo["organisation_code"].values

closed_tree = cKDTree(closed_coords)

# Batch query: for every unique panel practice with coordinates, find k=2 nearest
# closed practices (k=2 so we can skip the practice itself if it's in closed_panel).
k2 = min(2, len(closed_codes))
dists2, idxs2 = closed_tree.query(panel_coords, k=k2)
if k2 == 1:
    dists2 = dists2[:, np.newaxis]
    idxs2  = idxs2[:, np.newaxis]

gvar_nearest_map: dict[str, int] = {}
dist_nearest_map: dict[str, float] = {}
for i, pc in enumerate(panel_codes):
    for j in range(k2):
        if closed_codes[idxs2[i, j]] != pc:
            gvar_nearest_map[pc] = int(closed_years[idxs2[i, j]])
            dist_nearest_map[pc] = float(dists2[i, j])
            break
    # If all k2 candidates were itself (only happens when k2=1 and it's a closed
    # practice with no other closed practice nearby), fall back to index 0.
    if pc not in gvar_nearest_map:
        gvar_nearest_map[pc] = int(closed_years[idxs2[i, 0]])
        dist_nearest_map[pc] = float(dists2[i, 0])

panel["gvar_nearest"] = panel["practice_code"].map(gvar_nearest_map).fillna(10000).astype(int)
panel["dist_nearest_closed"] = panel["practice_code"].map(dist_nearest_map)


# ── Summary ───────────────────────────────────────────────────────────────────
def n_practices(mask):
    return panel.loc[mask, "practice_code"].nunique()

print(f"\nSummary:")
print(f"  treated=1          : {panel['treated'].sum():,} obs | "
      f"{n_practices(panel['treated']==1):,} practices")
print(f"  treated_nearest=1  : {panel['treated_nearest'].sum():,} obs | "
      f"{n_practices(panel['treated_nearest']==1):,} practices")
print(f"  gvar != 10000      : {(panel['gvar'] != 10000).sum():,} obs")
print(f"  gvar_nearest != 10000: {(panel['gvar_nearest'] != 10000).sum():,} obs")
print(f"  gvar_nearest year range: "
      f"{panel.loc[panel['gvar_nearest']!=10000,'gvar_nearest'].min()}–"
      f"{panel.loc[panel['gvar_nearest']!=10000,'gvar_nearest'].max()}")
print(f"  dist_nearest_closed    : "
      f"mean={panel['dist_nearest_closed'].mean():.4f}°  "
      f"min={panel['dist_nearest_closed'].min():.4f}°  "
      f"max={panel['dist_nearest_closed'].max():.4f}°")

# Spot-check a few treated_nearest practices
print("\nSpot-check treated_nearest (sample):")
sample_pcs = panel.loc[panel["treated_nearest"]==1, "practice_code"].drop_duplicates().head(3)
for pc in sample_pcs:
    ny = nearest_map[pc]
    rows = panel[panel["practice_code"]==pc][["year","treated_nearest"]].to_string(index=False)
    print(f"  {pc}  nearest_closure_year={ny}\n{rows}")


# ── Drop post-closure rows for closed practices ───────────────────────────────
# Some surveys were still sent to practices in the year they (or shortly after)
# closed. Remove any row where the practice itself has already closed
# (year >= its own close_year). Nearby-practice treatment rows are unaffected.
panel["_cy"] = panel["practice_code"].map(close_year_map)
post_closure_mask = panel["_cy"].notna() & (panel["year"] >= panel["_cy"])
n_dropped = post_closure_mask.sum()
panel = panel[~post_closure_mask].drop(columns=["_cy"])
print(f"\nDropped {n_dropped:,} post-closure rows from {panel['practice_code'].nunique():,} remaining practices")


# ── Save ──────────────────────────────────────────────────────────────────────
lead_cols  = ["practice_code", "year", "treated", "treated_nearest",
              "gvar", "gvar_nearest", "dist_nearest_closed", "msoa21"]
other_cols = [c for c in panel.columns
              if c not in lead_cols + ["lat", "long"]]
panel = panel[lead_cols + other_cols]

panel.to_csv(PANEL_PATH, index=False)
print(f"\nSaved → {os.path.abspath(PANEL_PATH)}")
