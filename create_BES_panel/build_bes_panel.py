"""
Build the BES analysis panel (data/bes_analysis.parquet).

Combines the two-notebook pipeline into a single script:

  Step 1  Convert the raw BES wide-format tab file to a long panel.
          Source: bes_panel_ukds_w1w25_v1.tab (Waves 1–25)
          Each wave's columns are extracted and renamed; the 25 waves are
          concatenated into a single long panel indexed by (id, wave).
          MSOA21CD and LAD22CD are added from msoa_lookup.csv.

  Step 2  Add treatment variables, covariates, and outcome variables.
          - GP practice closure dates per MSOA11 (from data/epraccur.csv)
          - Outcome variables: RRW_vote, Labour_vote, etc.
          - ONS local authority covariates (from data/ons_data_all.xlsx)
          - Treatment indicator and gvar (staggered DiD group variable)
          - Past-vote controls by election period
          - Immigration covariates (merge_immigration_statistics.py)
          - IMD deprivation scores (IMD_panel.py)

Output:
  data/bes_analysis.parquet
"""

import os
import sys
import numpy as np
import pandas as pd
import polars as pl

CODE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE     = os.path.join(CODE_DIR, "..", "data")

# Helper modules (merge_immigration_statistics.py, IMD_panel.py) live in
# CODE_DIR and use ../data/ relative paths, so we chdir there before importing.
_orig_dir = os.getcwd()
os.chdir(CODE_DIR)
sys.path.insert(0, CODE_DIR)

import merge_immigration_statistics as mis
import IMD_panel

os.chdir(_orig_dir)


# ── Step 1: Wide BES tab → long panel ────────────────────────────────────────

print("Reading BES tab file …")
raw = pd.read_csv(
    os.path.join(CODE_DIR, "bes_panel_ukds_w1w25_v1.tab"),
    sep="\t",
)
print(f"  Raw shape: {raw.shape} (respondents × variables)")


def wave_to_panel(df: pd.DataFrame, wave: int) -> pd.DataFrame:
    """Extract one wave's data and rename columns to drop the W{wave} suffix."""
    wave_df = df[pd.to_numeric(df[f"wave{wave}"], errors="coerce") == 1].copy()
    ids         = wave_df["id"].values
    waves_taken = wave_df["waves_taken"].values

    # Keep columns that belong to this wave OR are constant past-vote columns
    cols_mask = (
        (wave_df.columns.str.split("W").str.get(1) == str(wave)) |
        wave_df.columns.str.contains("p_past_vote_")
    )
    wave_df = wave_df.loc[:, list(cols_mask)]
    wave_df = wave_df.rename(columns=lambda x: x.replace(f"W{wave}", ""))

    # Drop any remaining wave-specific columns from other waves
    wave_df = wave_df.loc[:, ~wave_df.columns.str.contains("W")].reset_index(drop=True)

    wave_df["id"]          = ids
    wave_df["waves_taken"] = waves_taken
    wave_df["wave"]        = wave
    return wave_df


print("Converting to long panel (waves 1–25) …")
frames = []
for w in range(1, 26):
    wdf = wave_to_panel(raw, w)
    print(f"  Wave {w:2d}: {len(wdf):,} respondents", flush=True)
    frames.append(wdf)

ndf = pd.concat(frames, ignore_index=True)
print(f"Long panel shape: {ndf.shape}")

# Merge back constant columns (no W{n} suffix in the raw file) that are
# needed downstream but not extracted by the wave-column mask.
_const_needed = ["msoa11", "country"]
_const_missing = [c for c in _const_needed if c in raw.columns and c not in ndf.columns]
if _const_missing:
    print(f"  Merging back constant columns: {_const_missing}")
    ndf = ndf.merge(
        raw[["id"] + _const_missing].drop_duplicates("id"),
        on="id", how="left",
    )

# Add MSOA21CD and LAD22CD from pre-built lookup (msoa11 → MSOA21CD, LAD22CD)
print("Adding MSOA21CD and LAD22CD from msoa_lookup.csv …")
msoa_lookup = pl.read_csv(os.path.join(CODE_DIR, "msoa_lookup.csv"))

# Slim ndf to only the columns actually needed before polars conversion.
# The full ndf has ~2,500 wave-specific columns, many of which have mixed
# string/numeric content that triggers pyarrow type-inference errors.
_NEEDED = [
    "id", "wave", "waves_taken", "msoa11", "country", "starttime",
    "turnoutUKGeneral", "generalElectionVote", "partyIdSqueeze",
    "immigEcon", "immigCultural", "redistSelf", "pcon_code",
    "p_ethnicity", "p_edlevelUni", "p_socgrade", "p_work_stat", "p_marital",
    "ptvLab", "ptvUKIP", "p_gross_household", "p_gross_personal",
    "enviroProtection", "privatTooFar",
    "p_past_vote_2010", "p_past_vote_2015", "p_past_vote_2017", "p_past_vote_2019",
    "econPersonalRetro", "econGenRetro", "p_disability", "p_housing",
]
ndf_slim = ndf[[c for c in _NEEDED if c in ndf.columns]].copy()
# Parse starttime as datetime (values like "3/3/2014 19:59:22" are US-format
# strings that polars cast(pl.Datetime) cannot handle directly).
if "starttime" in ndf_slim.columns:
    ndf_slim["starttime"] = pd.to_datetime(ndf_slim["starttime"], errors="coerce")
# Coerce remaining object-dtype columns to numeric (handles empty-string missings
# and avoids pyarrow type-inference errors). Keep string-ID columns as-is.
_STR_KEEP = {"msoa11", "pcon_code"}
for col in ndf_slim.select_dtypes("object").columns:
    if col not in _STR_KEEP:
        ndf_slim[col] = pd.to_numeric(ndf_slim[col], errors="coerce")
print(f"  Slim shape: {ndf_slim.shape}")

bes = pl.DataFrame(ndf_slim).join(msoa_lookup, on="msoa11", how="left")

# Cast potentially-string numeric columns to Float64 so that is_in() and
# comparison operators work regardless of how the tab file was parsed.
_NUMERIC = [
    "generalElectionVote", "turnoutUKGeneral", "partyIdSqueeze",
    "p_past_vote_2010", "p_past_vote_2015", "p_past_vote_2017", "p_past_vote_2019",
    "immigEcon", "immigCultural", "redistSelf", "enviroProtection", "privatTooFar",
    "econPersonalRetro", "econGenRetro", "p_ethnicity", "p_edlevelUni", "p_socgrade",
    "p_work_stat", "p_marital", "p_gross_household", "p_gross_personal",
    "p_disability", "p_housing", "ptvLab", "ptvUKIP", "country",
]
bes = bes.with_columns([
    pl.col(c).cast(pl.Float64, strict=False)
    for c in _NUMERIC if c in bes.columns
])


# ── Step 2a: Load epraccur → first GP closure per MSOA11 ─────────────────────

print("Loading epraccur …")
postcodes = pl.read_parquet(os.path.join(BASE, "postcodes_2023.parquet"))

EPRACCUR_COLS = {
    0: "organisation_code", 1: "name", 9: "postcode",
    10: "open_date", 11: "close_date", 12: "status_code", 25: "prescribing_setting",
}
gp_raw = pd.read_csv(
    os.path.join(BASE, "epraccur.csv"),
    header=None, dtype=str, usecols=list(EPRACCUR_COLS.keys()),
).rename(columns=EPRACCUR_COLS)

gp_raw = gp_raw[gp_raw["prescribing_setting"] == "4"].copy()

def fix_date(col: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(col, errors="coerce")
    as_str  = numeric.astype("Int64").astype(str).replace("<NA>", "")
    return pd.to_datetime(as_str, format="%Y%m%d", errors="coerce")

gp_raw["open_date"]  = fix_date(gp_raw["open_date"])
gp_raw["close_date"] = fix_date(gp_raw["close_date"])

for col in gp_raw.select_dtypes("object").columns:
    gp_raw[col] = gp_raw[col].astype(str)

gp_pl = pl.DataFrame(gp_raw).with_columns(pl.col(pl.String).replace("nan", None))
gp_pl = gp_pl.join(postcodes, left_on="postcode", right_on="pcds", how="left")

# First closure date per MSOA11 (replicating notebook: .first() within group_by)
first_closure = (
    gp_pl
    .group_by(["msoa11"])
    .agg(
        pl.col("close_date").first().alias("first_close_date"),
        pl.col("open_date").last().alias("last_open_date"),
    )
    .with_columns([
        pl.col("first_close_date").cast(pl.Datetime).dt.year().alias("first_close_year"),
        pl.col("last_open_date").cast(pl.Datetime).dt.year().alias("last_open_year"),
    ])
    .filter(pl.col("msoa11").is_not_null())
)


# ── Step 2b: Outcome variables and column selection ───────────────────────────

print("Creating outcome variables …")

# Add oslaua_code from postcodes (msoa11 → oslaua)
msoa_postcodes = postcodes.unique("msoa11").rename({"oslaua": "oslaua_code"})
bes = bes.join(msoa_postcodes.select(["oslaua_code", "msoa11"]), on="msoa11", how="left")

bes = bes.with_columns(pl.col("generalElectionVote").replace(9999.0, None))

bes = bes.with_columns([
    pl.col("starttime").cast(pl.Datetime).alias("date"),
    (pl.col("generalElectionVote").is_in([6, 8, 12])).cast(pl.Int32).alias("RRW_vote"),
    (pl.col("generalElectionVote") == 2).cast(pl.Int32).alias("Labour_vote"),
    (pl.col("generalElectionVote") == 1).cast(pl.Int32).alias("Conservative_vote"),
    (pl.col("generalElectionVote") == 3).cast(pl.Int32).alias("LibDem_vote"),
    (pl.col("generalElectionVote") == 7).cast(pl.Int32).alias("green_vote"),
    (pl.col("turnoutUKGeneral").is_in([1, 2])).cast(pl.Int32).alias("unlikely_to_turnout"),
    (pl.col("partyIdSqueeze").is_in([6, 8, 12])).cast(pl.Int32).alias("RRW_squeeze_vote_closest"),
    (pl.col("p_past_vote_2010").is_in([6, 8])).cast(pl.Int32).alias("RRW_past_vote_2010"),
    (pl.col("p_past_vote_2015").is_in([6, 8])).cast(pl.Int32).alias("RRW_past_vote_2015"),
    (pl.col("p_past_vote_2017").is_in([6, 8])).cast(pl.Int32).alias("RRW_past_vote_2017"),
    (pl.col("p_past_vote_2019").is_in([12])).cast(pl.Int32).alias("RRW_past_vote_2019"),
])

COLS_TO_KEEP = [
    "turnoutUKGeneral", "date", "generalElectionVote",
    "RRW_vote", "id", "wave", "country", "immigEcon", "immigCultural",
    "redistSelf", "waves_taken", "pcon_code", "LAD22CD", "p_ethnicity",
    "p_edlevelUni", "p_socgrade", "p_work_stat", "p_marital",
    "Labour_vote", "unlikely_to_turnout", "ptvLab", "ptvUKIP",
    "Conservative_vote", "LibDem_vote", "p_gross_household",
    "p_gross_personal", "RRW_squeeze_vote_closest", "enviroProtection",
    "privatTooFar", "p_past_vote_2010", "p_past_vote_2015",
    "p_past_vote_2019", "p_past_vote_2017", "MSOA21CD", "msoa11",
    "econPersonalRetro", "econGenRetro", "green_vote",
    "p_disability", "p_housing", "oslaua_code",
]
# Guard against any column that didn't survive the wave extraction
COLS_TO_KEEP = [c for c in COLS_TO_KEEP if c in bes.columns]

bes_df = (
    bes
    .select(COLS_TO_KEEP)
    .join(first_closure, on="msoa11", how="left")
)


# ── Step 2c: Year and ONS local authority covariates ──────────────────────────

print("Merging ONS covariates …")

bes_df = bes_df.with_columns(pl.col("date").dt.year().cast(pl.Int32).alias("year"))


def get_ons_data(sheet: str, colname: str, skiprows: int = 5) -> pl.DataFrame:
    ons = pd.read_excel(
        os.path.join(BASE, "ons_data_all.xlsx"),
        sheet_name=sheet, skiprows=skiprows,
    )
    # The value column may have different names across sheets
    for src in ("Value (£)", "Value (%)", "Value (per 1,000 stock)"):
        if src in ons.columns:
            ons = ons.rename(columns={src: colname})
            break
    ons["year"] = ons["Period"].astype(str).str[:4].astype(int)
    f = (
        pl.DataFrame(ons)
        .select(["year", colname, "Area code"])
        .rename({"Area code": "oslaua_code"})
        .with_columns(pl.col("year").cast(pl.Int32))
        .sort(["oslaua_code", "year"])
    )
    return f


inactivity_data = get_ons_data("1",  "inactivity_rate")
employment_data = get_ons_data("3",  "employment_rate")
claimant_data   = get_ons_data("6",  "claimant_count")
gdi_data        = get_ons_data("7",  "gross_disposable_income")
pay_data        = get_ons_data("8",  "gross_median_weekly_pay", skiprows=7)
gdp_data        = get_ons_data("11", "GDP_per_person")
housing_data    = get_ons_data("21", "net_additions_to_housing_stock")

bes_df = bes_df.sort(["oslaua_code", "year"])
for data, suffix in [
    (inactivity_data, "_inactivity_rate"),
    (employment_data, "_employment_rate"),
    (claimant_data,   "_claimant_count"),
    (gdi_data,        "_gross_disposable_income"),
    (pay_data,        "_gross_median_weekly_pay"),
    (gdp_data,        "_gdp_per_capita"),
    (housing_data,    "_net_additions_to_housing_stock"),
]:
    bes_df = bes_df.join(data, on=["year", "oslaua_code"], suffix=suffix, how="left")


# ── Step 2d: Treatment, gvar, past vote ───────────────────────────────────────

print("Creating treatment and gvar variables …")

bes_df = bes_df.with_columns([
    pl.when(pl.col("year") > pl.col("first_close_year")).then(1).otherwise(0).alias("treatment"),
    pl.when(pl.col("last_open_year") >= pl.col("first_close_year")).then(1).otherwise(0).alias("open_after_close"),
])

# Convert to pandas for group-level gvar computation and past-vote logic
bes_df = bes_df.to_pandas()
bes_df = bes_df[bes_df["country"].isin([1])].copy()   # England only

id_to_first_wave = (
    bes_df[bes_df["treatment"] == 1]
    .groupby("msoa11")["year"]
    .min()
    .astype(int)
    .to_dict()
)
bes_df["gvar"] = bes_df["msoa11"].map(id_to_first_wave).fillna(10000)

# Sentinel → NaN
SENTINEL_COLS = {
    "immigEcon": [9999], "immigCultural": [9999], "redistSelf": [9999],
    "enviroProtection": [9999], "privatTooFar": [9999],
    "econPersonalRetro": [9999], "econGenRetro": [9999],
    "p_gross_personal": [9999, 15],
}
for col, vals in SENTINEL_COLS.items():
    if col in bes_df.columns:
        bes_df[col] = bes_df[col].replace(vals, np.nan)

bes_df["unemployed"]         = np.where((bes_df["p_work_stat"] == 6) & (bes_df["p_work_stat"] != 8), 1, 0)
bes_df["employed_full_time"] = np.where(bes_df["p_work_stat"] == 1, 1, 0)

# Past RRW vote by election period
bes_df["past_vote_RRW_2010"] = np.where(
    bes_df["p_past_vote_2010"].isin([6, 8]) & (bes_df["date"] < "2015-05-07"), 1, 0)
bes_df["past_vote_RRW_2010"] = np.where(
    bes_df["p_past_vote_2010"] == 9999, np.nan, bes_df["past_vote_RRW_2010"])

bes_df["past_vote_RRW_2015"] = np.where(
    bes_df["p_past_vote_2015"].isin([6, 8]) &
    (bes_df["date"] < "2017-06-08") & (bes_df["date"] > "2015-05-07"), 1, 0)
bes_df["past_vote_RRW_2015"] = np.where(
    bes_df["p_past_vote_2015"] == 9999, np.nan, bes_df["past_vote_RRW_2015"])

bes_df["past_vote_RRW_2017"] = np.where(
    bes_df["p_past_vote_2017"].isin([6, 8]) &
    (bes_df["date"] < "2019-12-12") & (bes_df["date"] > "2017-06-08"), 1, 0)
bes_df["past_vote_RRW_2017"] = np.where(
    bes_df["p_past_vote_2017"] == 9999, np.nan, bes_df["past_vote_RRW_2017"])

bes_df["past_vote_RRW_2019"] = np.where(
    bes_df["p_past_vote_2019"].isin([12]) & (bes_df["date"] > "2019-12-12"), 1, 0)
bes_df["past_vote_RRW_2019"] = np.where(
    bes_df["p_past_vote_2019"] == 9999, np.nan, bes_df["past_vote_RRW_2019"])

bes_df["past_vote_RRW"] = bes_df[[
    "past_vote_RRW_2010", "past_vote_RRW_2015",
    "past_vote_RRW_2017", "past_vote_RRW_2019",
]].sum(axis=1)


# ── Step 2e: Immigration and IMD covariates ───────────────────────────────────

os.chdir(CODE_DIR)   # helper modules use ../data/ relative paths

print("Merging immigration covariates …")
bes_df = mis.merge_immigration_covariates_with_BES_data(bes_df)
bes_df = mis.get_proportion_of_migrants(bes_df)

print("Merging IMD data …")
bes_df = IMD_panel.merge_with_bes(bes_df)

os.chdir(_orig_dir)


# ── Step 2f: Final cleaning ───────────────────────────────────────────────────

print("Final cleaning …")

bes_df = bes_df.with_columns([
    pl.col("ptvukip").replace(9999, None),
    pl.col("ptvlab").replace(9999, None),
])

bes_df = bes_df.filter(pl.col("msoa11").is_not_null())

bes_df = bes_df.with_columns([
    pl.col("inflow_longterm_international_migration").cast(pl.Float64),
    pl.col("migrant_gp_registrations").cast(pl.Float64),
    pl.col("lad_population_estimate").cast(pl.Float64),
])

bes_df = bes_df.with_columns([
    (pl.col("inflow_longterm_international_migration") /
     pl.col("lad_population_estimate")).alias("international_migration_per_pop"),
    (pl.col("migrant_gp_registrations") /
     pl.col("lad_population_estimate")).alias("migrant_gp_registrations_per_pop"),
])

bes_df = bes_df.with_columns([
    pl.col("p_socgrade").replace([7, 8], None),
    pl.col("p_gross_personal").replace([15, 16], None),
    pl.col("p_gross_household").replace([17, 16], None),
])


# ── Save ──────────────────────────────────────────────────────────────────────

out_path = os.path.join(BASE, "bes_analysis.parquet")
bes_df.write_parquet(out_path)

print(f"\nFinal panel shape: {bes_df.shape}")
print(f"  Unique IDs : {bes_df['id'].n_unique():,}")
print(f"  Waves      : {sorted(bes_df['wave'].unique().to_list())}")
print(f"Saved → {os.path.abspath(out_path)}")
