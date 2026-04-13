"""
Create a GP Patient Survey panel dataset (2012-2023).

Indexed by (practice_code, year), with all available variables.
Also produces a codebook CSV mapping variable names to descriptions.

Output:
  data/GP patient survey/gp_panel.csv
  data/GP patient survey/gp_panel_codebook.csv
"""

import os
import re
import pandas as pd

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "GP patient survey"
)
DATA_DIR = BASE_DIR
VAR_DIR  = os.path.join(BASE_DIR, "variable_lists")

YEARS = list(range(2012, 2024))   # 2012 … 2023


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_variable_list(year: int) -> dict[str, str]:
    """Return {lowercase_varname: description} from a variable-list CSV.

    Falls back to 2022 when no file exists (e.g. 2023).
    The 2022 file has many spurious extra empty columns; we handle that by
    reading only the first two columns.
    """
    path = os.path.join(VAR_DIR, f"{year}.csv")
    if not os.path.exists(path):
        path = os.path.join(VAR_DIR, "2022.csv")   # best available proxy

    df = pd.read_csv(path, usecols=[0, 1], header=0,
                     names=["variable", "description"],
                     dtype=str, encoding="latin-1")
    df = df.dropna(subset=["variable"])
    df["variable"] = df["variable"].str.strip().str.lower()
    df["description"] = df["description"].str.strip()
    return dict(zip(df["variable"], df["description"]))


def load_year(year: int) -> pd.DataFrame:
    """Load one year's data CSV, normalise column names, add year column."""
    path = os.path.join(DATA_DIR, f"{year}.csv")
    df = pd.read_csv(path, dtype=str, encoding="utf-8-sig", low_memory=False)

    # Strip BOM from column names (utf-8-sig handles it, but be safe)
    df.columns = [c.strip().lstrip("\ufeff") for c in df.columns]

    # Normalise to lowercase
    df.columns = df.columns.str.lower()

    # Make sure practice_code exists (it always does under various cases)
    if "practice_code" not in df.columns:
        raise KeyError(f"{year}.csv has no 'practice_code' column. "
                       f"Columns: {list(df.columns[:10])}")

    df["year"] = year
    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    all_frames = []
    codebook_rows = []          # (variable, description, first_year, last_year)
    codebook_map: dict[str, dict] = {}   # varname → {description, first_year, last_year}

    print(f"Loading {len(YEARS)} years of data …")
    for year in YEARS:
        print(f"  {year}", end=" ", flush=True)
        df   = load_year(year)
        vmap = load_variable_list(year)

        # Record codebook entries
        for col in df.columns:
            if col in ("practice_code", "year"):
                continue
            desc = vmap.get(col, "")
            if col not in codebook_map:
                codebook_map[col] = {
                    "description": desc,
                    "first_year":  year,
                    "last_year":   year,
                }
            else:
                if not codebook_map[col]["description"] and desc:
                    codebook_map[col]["description"] = desc
                codebook_map[col]["last_year"] = year

        all_frames.append(df)
        print(f"({len(df):,} practices)", flush=True)

    print("\nConcatenating …")
    panel = pd.concat(all_frames, axis=0, ignore_index=True, sort=False)

    # Move practice_code and year to front, set as index
    non_index = [c for c in panel.columns if c not in ("practice_code", "year")]
    panel = panel[["practice_code", "year"] + non_index]
    panel = panel.set_index(["practice_code", "year"]).sort_index()

    print(f"Panel shape: {panel.shape}")
    print(f"  Practices  : {panel.index.get_level_values('practice_code').nunique():,}")
    print(f"  Years      : {sorted(panel.index.get_level_values('year').unique().tolist())}")
    print(f"  Variables  : {panel.shape[1]:,}")

    # ── Save panel ────────────────────────────────────────────────────────────
    out_panel = os.path.join(DATA_DIR, "gp_panel.csv")
    panel.to_csv(out_panel)
    print(f"\nPanel saved  → {out_panel}")

    # ── Save codebook ─────────────────────────────────────────────────────────
    codebook = pd.DataFrame([
        {"variable": k, **v}
        for k, v in codebook_map.items()
    ])
    codebook = codebook[["variable", "description", "first_year", "last_year"]]
    out_cb = os.path.join(DATA_DIR, "gp_panel_codebook.csv")
    codebook.to_csv(out_cb, index=False)
    print(f"Codebook saved → {out_cb}")
    print(f"  {len(codebook):,} unique variables documented")


if __name__ == "__main__":
    main()
