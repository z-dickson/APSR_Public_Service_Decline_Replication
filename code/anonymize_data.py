
# anonymize_data.py — reduce panel datasets to analysis-ready, anonymized form
#
# Run this script ONCE on the full panel files to produce the anonymized
# versions included in the public replication package.  The script:
#
#   1. Retains only the columns required by the analysis scripts.
#   2. Replaces all geographic identifier strings (MSOA codes, LAD codes,
#      practice codes) with consistent sorted integers, so that fixed-effects
#      estimators still work but individual locations cannot be identified.
#
# The three files are overwritten in place:
#   ../data/bes_analysis.parquet
#   ../data/gp_patient_survey_panel.parquet
#   ../data/gp_practice_registrations_panel.csv
#
# IMPORTANT: run this script against the FULL (non-anonymized) versions of
# these files.  It is a one-way transformation — the original geographic codes
# cannot be recovered from the integer encodings.

import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')


def make_int_map(series):
    """Map each unique non-null value in *series* to a 1-based sorted integer."""
    unique_vals = sorted(v for v in series.dropna().unique())
    return {v: i + 1 for i, v in enumerate(unique_vals)}


# ── 1. BES analysis panel ─────────────────────────────────────────────────────
#
# Columns kept are the union of all columns used by bes_analysis.R and
# bes_moderation_analysis.R.  msoa11 (MSOA 2011 codes like "E02003594") is
# replaced with a 1-based integer that is consistent across all rows.

BES_KEEP = [
    'id',                                  # respondent ID (already numeric)
    'wave',                                # BES wave number (1–25)
    'year',                                # calendar year of the wave
    'gvar',                                # cohort: first treatment year (10000 = never)
    'msoa11',                              # MSOA 2011 code → integer
    'treatment',                           # binary treatment indicator
    'open_after_close',                    # robustness flag
    'rrw_vote',                            # outcome: right-wing vote intention
    'past_vote_rrw',                       # outcome: recalled right-wing vote
    'conservative_vote',                   # outcome: Conservative vote intention
    'labour_vote',                         # outcome: Labour vote intention
    'libdem_vote',                         # outcome: Lib Dem vote intention
    'green_vote',                          # outcome: Green vote intention
    'IMD_Score',                           # area deprivation (control)
    'employment_rate',                     # local employment rate (control)
    'international_migration_per_pop',     # migration flow per population (control)
    'migrant_gp_registrations_per_pop',    # migrant GP registrations per pop (control)
]

bes_path = os.path.join(DATA_DIR, 'bes_analysis.parquet')
bes = pd.read_parquet(bes_path)
present = [c for c in BES_KEEP if c in bes.columns]
missing = [c for c in BES_KEEP if c not in bes.columns]
if missing:
    print(f"  WARNING: BES columns not found and skipped: {missing}")
bes = bes[present].copy()

msoa11_map = make_int_map(bes['msoa11'])
bes['msoa11'] = bes['msoa11'].map(msoa11_map)

bes.to_parquet(bes_path, index=False)
print(f"BES:           {bes.shape[0]:>7,} rows × {bes.shape[1]:>2} columns → {bes_path}")


# ── 2. GP Patient Survey panel ────────────────────────────────────────────────
#
# Columns kept are the union of all columns used by gpps_analysis.R and
# honest_did_gpps.R.  practice_code, msoa21 (MSOA 2021), and oslaua (LAD)
# are each replaced with independent 1-based sorted integers.

GPPS_KEEP = [
    'practice_code',                                         # GP practice → integer
    'year',
    'treated',                                               # binary treatment
    'treated_nearest',                                       # binary nearest-practice treatment
    'gvar',                                                  # cohort: first closure year
    'gvar_nearest',                                          # cohort: nearest-practice version
    'msoa21',                                                # MSOA 2021 code → integer
    'oslaua',                                                # LAD code → integer
    'treatment',
    'positive_overall_experience_with_gp_practice',          # outcome
    'negative_overall_experience_making_an_appointment',     # outcome
    'IMD_Score',                                             # deprivation (control)
    'unemployment_rate',                                     # unemployment rate (control)
    'inflow_longterm_international_migration_proportion',    # migration inflow (control)
    'migrant_gp_registrations_proportion',                   # migrant registrations (control)
]

gpps_path = os.path.join(DATA_DIR, 'gp_patient_survey_panel.parquet')
gpps = pd.read_parquet(gpps_path)
present = [c for c in GPPS_KEEP if c in gpps.columns]
missing = [c for c in GPPS_KEEP if c not in gpps.columns]
if missing:
    print(f"  WARNING: GPPS columns not found and skipped: {missing}")
gpps = gpps[present].copy()

for col in ('practice_code', 'msoa21', 'oslaua'):
    col_map = make_int_map(gpps[col])
    gpps[col] = gpps[col].map(col_map)

gpps.to_parquet(gpps_path, index=False)
print(f"GPPS:          {gpps.shape[0]:>7,} rows × {gpps.shape[1]:>2} columns → {gpps_path}")


# ── 3. GP practice registrations panel ───────────────────────────────────────
#
# fig3_GP_closures_patients.py uses: gp_practice_code (for deduplication),
# year, close_year, patients_before_close.  gp_practice_code is replaced with
# a 1-based sorted integer.

REG_KEEP = [
    'gp_practice_code',      # NHS practice code → integer
    'year',
    'close_year',
    'patients_before_close',
]

reg_path = os.path.join(DATA_DIR, 'gp_practice_registrations_panel.csv')
reg = pd.read_csv(reg_path)
present = [c for c in REG_KEEP if c in reg.columns]
missing = [c for c in REG_KEEP if c not in reg.columns]
if missing:
    print(f"  WARNING: registrations columns not found and skipped: {missing}")
reg = reg[present].copy()

gp_code_map = make_int_map(reg['gp_practice_code'])
reg['gp_practice_code'] = reg['gp_practice_code'].map(gp_code_map)

reg.to_csv(reg_path, index=False)
print(f"Registrations: {reg.shape[0]:>7,} rows × {reg.shape[1]:>2} columns → {reg_path}")

print("\nDone. All files anonymized and saved.")
