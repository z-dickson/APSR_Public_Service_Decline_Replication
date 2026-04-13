"""
GPPS_MAIN.py — Master script for building the GP Patient Survey analysis panel.

Runs the following steps in order:

  1. create_gp_panel.py
       Loads raw annual CSVs (2012–2023) and stacks them into a single panel
       indexed by (practice_code, year).
       Output: data/GP patient survey/gp_panel.csv

  2. add_treatment_indicator.py
       Adds treatment indicators and group variables using epraccur closure dates
       (GP practices only, Prescribing setting == 4).
       Adds: treated, treated_nearest, gvar, gvar_nearest, msoa21
       Output: data/GP patient survey/gp_panel.csv  (updated in place)

  3. add_covariates_to_GPPS.py
       Creates outcome variables, merges external covariates (immigration,
       unemployment, GDI, anxiety, IMD), and saves the analysis-ready file.
       Output: data/gp_patient_survey_panel.parquet

Usage:
    python GPPS_MAIN.py
"""

import subprocess
import sys
import os
import time

CODE_DIR = os.path.dirname(os.path.abspath(__file__))

STEPS = [
    ("create_gp_panel.py",          "Build raw panel from annual CSVs"),
    ("add_treatment_indicator.py",   "Add treatment indicators and gvar"),
    ("add_covariates_to_GPPS.py",    "Add outcome variables and covariates"),
]


def run_step(script: str, description: str) -> None:
    path = os.path.join(CODE_DIR, script)
    print(f"\n{'='*60}")
    print(f"  STEP: {description}")
    print(f"  Script: {script}")
    print(f"{'='*60}")
    t0 = time.time()
    result = subprocess.run(
        [sys.executable, path],
        cwd=CODE_DIR,
    )
    elapsed = time.time() - t0
    if result.returncode != 0:
        print(f"\n[ERROR] {script} failed (exit code {result.returncode}).")
        sys.exit(result.returncode)
    print(f"\n  Done in {elapsed:.1f}s")


if __name__ == "__main__":
    print("GP Patient Survey — full pipeline")
    t_total = time.time()

    for script, description in STEPS:
        run_step(script, description)

    print(f"\n{'='*60}")
    print(f"  Pipeline complete in {time.time() - t_total:.1f}s")
    print(f"  Final output: data/gp_patient_survey_panel.parquet")
    print(f"{'='*60}")
