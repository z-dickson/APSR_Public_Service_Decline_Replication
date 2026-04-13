"""
Add outcome variables and covariates to the GP Patient Survey panel.

Reproduces the operations from gp_patient_survey.ipynb, excluding gvar
creation (already done in add_treatment_indicator.py).

Operations:
  1. Create renamed outcome variables from raw q columns
  2. Create combined (summed) outcome variables
  3. Add oslaua (local authority code) from gp_closures()
  4. Add MSOA-level first-closure and last-open dates
  5. Create MSOA-level treatment indicator and last_open_after_closure flag
  6. Merge immigration covariates
  7. Merge unemployment rate
  8. Merge gross disposable household income (+ log)
  9. Merge IMD scores

Input:
  data/GP patient survey/gp_panel.csv   (with treated/gvar columns already added)

Output:
  data/gp_patient_survey_panel.parquet
"""

import os
import sys
import numpy as np
import pandas as pd
import polars as pl

CODE_DIR  = os.path.dirname(os.path.abspath(__file__))
BASE      = os.path.join(CODE_DIR, "..", "data")
PANEL_IN  = os.path.join(CODE_DIR, "GP patient survey", "gp_panel.csv")
PANEL_OUT = os.path.join(BASE, "gp_patient_survey_panel.parquet")

# Helper modules use relative paths from code/, so keep cwd there while importing
_orig_dir = os.getcwd()
os.chdir(CODE_DIR)
sys.path.insert(0, CODE_DIR)

from get_practice_closures import gp_closures
import merge_immigration_statistics as mis
import IMD_panel

# Load GP closures while still in code/ (get_practice_closures uses ../data paths)
print("Loading GP practice data …")
gp = gp_closures()

os.chdir(_orig_dir)

# ── Load panel ────────────────────────────────────────────────────────────────
print("Loading panel …")
# q columns are stored as strings; cast the ones we need to float
OUTCOME_COLS = [
    "q28_1pct", "q28_2pct",
    "q18_5pct", "q18_4pct",
    "q25_5pct", "q25_4pct",
]
df = pl.read_csv(PANEL_IN, infer_schema_length=0)   # read all as Utf8 first

# Cast numeric columns needed for outcomes
df = df.with_columns([
    pl.col(c).cast(pl.Float64, strict=False) for c in OUTCOME_COLS
] + [
    pl.col("year").cast(pl.Int32),
    pl.col("treated").cast(pl.Int32),
    pl.col("treated_nearest").cast(pl.Int32),
    pl.col("gvar").cast(pl.Int32),
    pl.col("gvar_nearest").cast(pl.Int32),
])

print(f"  Panel shape: {df.shape}")


# ── 1. Create renamed outcome variables ───────────────────────────────────────
df = df.with_columns([
    pl.col("q28_1pct").alias("overall_experience_with_gp_practice_very_good"),
    pl.col("q28_2pct").alias("overall_experience_with_gp_practice_fairly_good"),
    pl.col("q18_5pct").alias("overall_experience_making_an_appointment_very_poor"),
    pl.col("q18_4pct").alias("overall_experience_making_an_appointment_fairly_poor"),
    pl.col("q25_5pct").alias("satisfaction_with_practice_appointment_times_very_dissatisfied"),
    pl.col("q25_4pct").alias("satisfaction_with_practice_appointment_times_fairly_dissatisfied"),
])

# Replace sentinel values (-97, -98) with null
OUTCOME_RENAMED = [
    "overall_experience_with_gp_practice_very_good",
    "overall_experience_with_gp_practice_fairly_good",
    "overall_experience_making_an_appointment_very_poor",
    "overall_experience_making_an_appointment_fairly_poor",
    "satisfaction_with_practice_appointment_times_very_dissatisfied",
    "satisfaction_with_practice_appointment_times_fairly_dissatisfied",
]
df = df.with_columns([
    pl.col(c).replace(-97, None).replace(-98, None) for c in OUTCOME_RENAMED
])


# ── 2. Create combined outcome variables ──────────────────────────────────────
df = df.with_columns([
    (pl.col("overall_experience_with_gp_practice_very_good")
     + pl.col("overall_experience_with_gp_practice_fairly_good"))
    .alias("positive_overall_experience_with_gp_practice"),

    (pl.col("overall_experience_making_an_appointment_very_poor")
     + pl.col("overall_experience_making_an_appointment_fairly_poor"))
    .alias("negative_overall_experience_making_an_appointment"),

    (pl.col("satisfaction_with_practice_appointment_times_very_dissatisfied")
     + pl.col("satisfaction_with_practice_appointment_times_fairly_dissatisfied"))
    .alias("dissatisfaction_with_practice_appointment_times"),
])


# ── 3. Add oslaua from gp_closures ────────────────────────────────────────────
print("Adding oslaua …")
df = df.join(
    gp.select(["organisation_code", "oslaua"]),
    left_on="practice_code",
    right_on="organisation_code",
    how="left",
)


# ── 4. MSOA-level first-closure and last-open dates ───────────────────────────
print("Computing MSOA-level closure dates …")
first_closure = (
    gp.filter(pl.col("close_date").is_not_null())
    .group_by("msoa21")
    .agg(pl.col("close_date").min().alias("first_close_date"))
)
last_open = (
    gp.group_by("msoa21")
    .agg(pl.col("open_date").max().alias("last_open_date"))
)
msoa_dates = (
    first_closure
    .join(last_open, on="msoa21", how="left")
    .with_columns([
        pl.col("first_close_date").cast(pl.Datetime),
        pl.col("last_open_date").cast(pl.Datetime),
    ])
    .filter(pl.col("msoa21").is_not_null())
)

df = df.join(msoa_dates, on="msoa21", how="left")


# ── 5. MSOA-level treatment indicator ─────────────────────────────────────────
df = df.with_columns([
    pl.col("first_close_date").cast(pl.Date).dt.year().alias("year_of_closure"),
    pl.col("last_open_date").cast(pl.Date).dt.year().alias("last_year_open"),
])
df = df.with_columns([
    pl.when(pl.col("year") >= pl.col("year_of_closure"))
    .then(1).otherwise(0)
    .alias("treatment"),

    pl.when(pl.col("last_year_open") > pl.col("year_of_closure"))
    .then(1).otherwise(0)
    .alias("last_open_after_closure"),
])
# Note: gvar already added by add_treatment_indicator.py — not recreated here.


# ── 6–10. Merge covariates (helper modules use cwd=code/) ────────────────────
os.chdir(CODE_DIR)

# 6. Immigration covariates
print("Merging immigration covariates …")
df = df.with_columns(pl.col("year").cast(pl.Int64))
df = mis.merge_immigration_covariates_with_GPPS_data(df)
df = mis.get_proportion_of_migrants(df)

# 7. Unemployment rate
print("Merging unemployment data …")
employ = pd.read_csv(os.path.join(BASE, "modelled-unemployment-table-data.csv"), skiprows=6)
employ = employ.melt(id_vars=["Area code", "Area name"], var_name="year", value_name="unemployment_rate")
employ = pl.DataFrame(employ).with_columns([
    pl.col("year").cast(pl.Int64),
    pl.col("unemployment_rate").cast(pl.Float32),
    pl.col("Area code").cast(pl.String),
])
df = df.join(
    employ.select(["Area code", "year", "unemployment_rate"]),
    left_on=["oslaua", "year"],
    right_on=["Area code", "year"],
    how="left",
)

# 8. Gross disposable household income (from ons_data_all.xlsx, sheet "7")
print("Merging GDI data …")
gdi = pd.read_excel(os.path.join(BASE, "ons_data_all.xlsx"), sheet_name="7", skiprows=5)
gdi = gdi.rename(columns={"Value (£)": "gross_disposable_income"})
gdi["year"] = gdi["Period"].astype(str).str[:4].astype(int)
gdi = gdi[["Area code", "year", "gross_disposable_income"]]
gdi = pl.DataFrame(gdi).with_columns([
    pl.col("year").cast(pl.Int64),
    pl.col("gross_disposable_income").cast(pl.Float32),
    pl.col("Area code").cast(pl.String),
])
df = df.join(
    gdi.select(["Area code", "year", "gross_disposable_income"]),
    left_on=["oslaua", "year"],
    right_on=["Area code", "year"],
    how="left",
)
df = df.with_columns(
    np.log(pl.col("gross_disposable_income") + 1).alias("log_gross_disposable_income")
)

# 9. IMD scores
print("Merging IMD data …")
df = df.with_columns(pl.col("year").cast(pl.Int32))
df = IMD_panel.merge_with_GPPS(df)

os.chdir(_orig_dir)


# ── Select final columns and save ─────────────────────────────────────────────
# Lead with identifiers, treatment, gvar, outcomes, then covariates.
# Drop raw q columns to keep the analysis file manageable.
lead = [
    "practice_code", "year",
    "treated", "treated_nearest",
    "gvar", "gvar_nearest",
    "msoa21", "oslaua",
    "treatment", "year_of_closure", "last_year_open", "last_open_after_closure",
    "positive_overall_experience_with_gp_practice",
    "overall_experience_with_gp_practice_very_good",
    "overall_experience_with_gp_practice_fairly_good",
    "overall_experience_making_an_appointment_very_poor",
    "overall_experience_making_an_appointment_fairly_poor",
    "negative_overall_experience_making_an_appointment",
    "satisfaction_with_practice_appointment_times_very_dissatisfied",
    "satisfaction_with_practice_appointment_times_fairly_dissatisfied",
    "dissatisfaction_with_practice_appointment_times",
]
# Append any covariate columns that were added (everything not already listed
# and not a raw q column)
raw_q = [c for c in df.columns if c.startswith("q") and c[1].isdigit()]
extra = [c for c in df.columns if c not in lead and c not in raw_q]
final_cols = [c for c in lead if c in df.columns] + extra

df = df.select(final_cols)

print(f"\nFinal panel shape: {df.shape}")
df.write_parquet(PANEL_OUT)
print(f"Saved → {os.path.abspath(PANEL_OUT)}")
