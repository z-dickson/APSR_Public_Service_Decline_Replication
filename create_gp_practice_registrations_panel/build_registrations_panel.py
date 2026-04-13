"""
Build the GP practice patient registration panel (2013–2022).

Reads annual patient registration CSVs from:
  gp practice registrations/gp_reg_YYYY.csv

Two raw file formats are handled:
  - 2013–2016: wide format; total patients in a TOTAL_ALL column,
    one row per practice.
  - 2017–2022: long format; filter to SEX=ALL and AGE=ALL to get
    the all-patients total (NUMBER_OF_PATIENTS), one row per practice.

Merges with data/epraccur.csv to add practice name, postcode,
open/close dates, and status code. Computes:
  close_year            – year extracted from close_date
  ever_closed           – True if the practice has any close_date in epraccur
  closed_by_year        – True if the practice had closed by that panel year
  patients_before_close – total_patients in the year immediately before closure
                          (0 if the practice did not close that year + 1)

Output:
  data/gp_practice_registrations_panel.csv
"""

import os
import pandas as pd

CODE_DIR  = os.path.dirname(os.path.abspath(__file__))
BASE      = os.path.join(CODE_DIR, "..", "data")
REG_DIR   = os.path.join(CODE_DIR, "gp practice registrations")
PANEL_OUT = os.path.join(BASE, "gp_practice_registrations_panel.csv")
EPRACCUR  = os.path.join(BASE, "epraccur.csv")

YEARS = list(range(2013, 2023))   # 2013 … 2022


# ── Load one year of registration data ───────────────────────────────────────

def read_reg(year: int) -> pd.DataFrame:
    """Return DataFrame with columns [gp_practice_code, year, total_patients]."""
    path = os.path.join(REG_DIR, f"gp_reg_{year}.csv")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = df.columns.str.lower().str.strip()

    if "sex" in df.columns and "age" in df.columns:
        # Long format (2017+): keep the all-patients total row only
        df = df[
            (df["sex"].str.upper() == "ALL") & (df["age"].str.upper() == "ALL")
        ].copy()
        df = df.rename(columns={"code": "gp_practice_code",
                                 "number_of_patients": "total_patients"})
    else:
        # Wide format (2013–2016): one row per practice, TOTAL_ALL is the total
        df = df.rename(columns={"total_all": "total_patients"})
        # 2013 uses GP_PRACTICE_CODE; already lowercased to gp_practice_code

    df["year"] = year
    df["gp_practice_code"] = df["gp_practice_code"].str.strip().str.upper()
    df["total_patients"] = pd.to_numeric(df["total_patients"], errors="coerce")
    return df[["gp_practice_code", "year", "total_patients"]].copy()


# ── Load epraccur ─────────────────────────────────────────────────────────────

def load_epraccur() -> pd.DataFrame:
    """
    Parse epraccur.csv (no header row) and return GP practices (prescribing
    setting == 4) with columns:
      organisation_code, name, postcode, open_date, close_date, status_code
    """
    # Column positions (0-indexed) in epraccur:
    #  0  organisation_code   9  postcode    10 open_date
    # 11  close_date         12  status_code 25 prescribing_setting
    COLS = {
        0:  "organisation_code",
        1:  "name",
        9:  "postcode",
        10: "open_date",
        11: "close_date",
        12: "status_code",
        25: "prescribing_setting",
    }
    raw = pd.read_csv(EPRACCUR, header=None, dtype=str, usecols=list(COLS.keys()))
    raw = raw.rename(columns=COLS)

    gp = raw[raw["prescribing_setting"] == "4"].copy()

    def fix_date(col: pd.Series) -> pd.Series:
        numeric = pd.to_numeric(col, errors="coerce")
        as_str  = numeric.astype("Int64").astype(str).replace("<NA>", "")
        return pd.to_datetime(as_str, format="%Y%m%d", errors="coerce")

    gp["open_date"]  = fix_date(gp["open_date"])
    gp["close_date"] = fix_date(gp["close_date"])
    gp["organisation_code"] = gp["organisation_code"].str.strip().str.upper()

    return gp[["organisation_code", "name", "postcode",
               "open_date", "close_date", "status_code"]].copy()


# ── Build panel ───────────────────────────────────────────────────────────────

print(f"Loading {len(YEARS)} years of registration data …")
frames = []
for year in YEARS:
    print(f"  {year}", end=" ", flush=True)
    df = read_reg(year)
    print(f"({len(df):,} practices)", flush=True)
    frames.append(df)

panel = pd.concat(frames, ignore_index=True)
print(f"\nPanel shape before merge: {panel.shape}")


# ── Merge with epraccur ────────────────────────────────────────────────────────

print("Loading epraccur …")
gp = load_epraccur()
print(f"  {len(gp):,} GP practices in epraccur")

panel = panel.merge(
    gp, left_on="gp_practice_code", right_on="organisation_code", how="left"
).drop(columns=["organisation_code"])


# ── Derived columns ────────────────────────────────────────────────────────────

panel["close_year"] = panel["close_date"].dt.year

closed_codes = set(gp.loc[gp["close_date"].notna(), "organisation_code"])
panel["ever_closed"] = panel["gp_practice_code"].isin(closed_codes)

panel["closed_by_year"] = (
    panel["close_date"].notna() & (panel["close_year"] <= panel["year"])
)

# patients_before_close: total_patients in the year immediately before closure
close_year_map = (
    gp.dropna(subset=["close_date"])
    .set_index("organisation_code")["close_date"]
    .dt.year
)
panel["_cy"] = panel["gp_practice_code"].map(close_year_map)
panel["patients_before_close"] = panel["total_patients"].where(
    panel["year"] == panel["_cy"] - 1, other=0.0
)
panel = panel.drop(columns=["_cy"])


# ── Reorder and save ──────────────────────────────────────────────────────────

col_order = [
    "total_patients", "gp_practice_code", "year",
    "name", "postcode", "open_date", "close_date",
    "close_year", "status_code",
    "ever_closed", "closed_by_year", "patients_before_close",
]
panel = (
    panel[col_order]
    .sort_values(["gp_practice_code", "year"])
    .reset_index(drop=True)
)

print(f"\nFinal panel shape: {panel.shape}")
print(f"  Practices  : {panel['gp_practice_code'].nunique():,}")
print(f"  Years      : {sorted(panel['year'].unique().tolist())}")
print(f"  Ever-closed: {panel['ever_closed'].sum():,} obs "
      f"({panel.loc[panel['ever_closed'], 'gp_practice_code'].nunique():,} practices)")
print(f"  Patients-before-close total: {panel['patients_before_close'].sum():,.0f}")

panel.to_csv(PANEL_OUT, index=False)
print(f"\nSaved → {os.path.abspath(PANEL_OUT)}")
