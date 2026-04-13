
# main.py — run all scripts for the replication package
#
# Executes each script in order and reports success or failure.
# Output figures and tables are saved to ../final_output_for_article/.
# Intermediate coefficient CSVs are saved to ../output_data_for_figures/.
#
# ── Replication modes ─────────────────────────────────────────────────────────
#
# BUILD_FROM_RAW = False  (default)
#   Skips the panel-build scripts and uses the pre-built, anonymized data files
#   in ../data/. Use this mode if you have obtained the data files from the
#   replication package. This is the recommended mode for most replicators.
#
# BUILD_FROM_RAW = True
#   Rebuilds all three panel datasets from raw input files before running the
#   analysis. This requires:
#     - GP Patient Survey CSVs (2012–2023) in
#       ../create_gp_patient_survey_panel/GP patient survey/
#     - GP registration CSVs (2013–2022) in
#       ../create_gp_practice_registrations_panel/gp practice registrations/
#     - BES tab file (bes_panel_ukds_w1w25_v1.tab) in
#       ../create_BES_panel/  (obtain from UK Data Service SN 8202)
#   See README.md for download instructions.
#
# ── Data preparation scripts (run only when BUILD_FROM_RAW = True) ────────────
#   create_gp_patient_survey_panel/GPPS_MAIN.py
#       Stacks raw GPPS CSVs (2012–2023), adds treatment vars, outcomes, and
#       covariates; saves data/gp_patient_survey_panel.parquet
#   create_gp_practice_registrations_panel/build_registrations_panel.py
#       Reads annual NHS registration CSVs; saves data/gp_practice_registrations_panel.csv
#   create_BES_panel/build_bes_panel.py
#       Converts wide BES tab file to long panel with treatment vars and
#       covariates; saves data/bes_analysis.parquet
#
# ── Figure and analysis scripts ───────────────────────────────────────────────
#   fig1_NHS_SPENDING_Yougov.py   — Figure 1:  YouGov NHS spending tracker
#   fig2_GP_closures_map.py       — Figure 2:  Map of GP practice closures (2013–2023)
#   fig3_GP_closures_patients.py  — Figure 3:  Patients affected & closures per year
#   gpps_analysis.R               — Sun & Abraham (2021) event-study models for GPPS
#   honest_did_gpps.R             — Honest DID sensitivity analysis for GPPS models
#   fig4_gpps_event_study.py      — Figure 4:  GPPS event-study coefficient plots
#   bes_analysis.R                — Sun & Abraham (2021) event-study models for BES
#   fig6_BES_event_study.py       — Figures 6, 9, A9, A13, A14, A16: BES event-study plots
#   fig7_text_analysis.py         — Figure 7:  NHS–immigration linkage in UKIP/Reform UK
#   bes_moderation_analysis.R     — Figure 8:  Treatment effect heterogeneity
#   figA1_GP_closures_heatmap.py  — Figure A1: Heatmap of GP closure locations
#   figA2_GP_closures_choropleth.py — Figure A2: Closures per 10k population by LAD
#   figA18_A19_bes_validation.py  — Figures A18–A19: BES vote intention vs actual results
#       Requires ../data/BES2024_W29_Panel_v29.1.dta (BES website, Waves 1–29)
#
# Note: figA2 requires ../data/local_authority_districts.geojson.
# If the file is missing the script will print download instructions and exit.
# R scripts are run via Rscript; Python scripts via the current interpreter.

import subprocess
import sys
import os

# ── Requirements check ────────────────────────────────────────────────────────
#
# Verifies that all Python packages and R packages are available before
# attempting to run any analysis scripts.  Set up the environment with:
#   conda env create -f ../environment.yml
#   conda activate nhs_replication
#   python -m spacy download en_core_web_sm
# R deps are installed automatically by ../requirements.R if missing.

def _check_requirements():
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # ── Python version ────────────────────────────────────────────────────────
    if sys.version_info >= (3, 13):
        print(f"ERROR: Python {sys.version_info.major}.{sys.version_info.minor} is not supported.")
        print("Several C-extension dependencies (spaCy, blis) do not have pre-built")
        print("wheels for Python 3.13+. Please use Python 3.11 or 3.12.")
        print("Create a compatible environment with:  conda env create -f environment.yml")
        sys.exit(1)

    # ── Python packages ───────────────────────────────────────────────────────
    _PYTHON_PKGS = [
        'pandas', 'numpy', 'polars', 'scipy', 'plotly', 'kaleido',
        'requests', 'geopandas', 'matplotlib', 'spacy', 'openpyxl', 'pyarrow',
    ]
    import importlib.util
    missing_py = [p for p in _PYTHON_PKGS if importlib.util.find_spec(p) is None]
    if missing_py:
        print("ERROR: The following Python packages are not installed:")
        for p in missing_py:
            print(f"    {p}")
        print("\nSet up the environment with:  conda env create -f environment.yml")
        sys.exit(1)

    # Check spaCy English model
    import spacy
    try:
        spacy.load("en_core_web_sm")
    except OSError:
        print("ERROR: spaCy model 'en_core_web_sm' is not installed.")
        print("Install it with:  python -m spacy download en_core_web_sm")
        sys.exit(1)

    print("Python packages OK.")

    # ── R packages ────────────────────────────────────────────────────────────
    r_script = os.path.join(repo_root, 'requirements.R')
    print("Checking R packages (installing any missing)...")
    result = subprocess.run(['Rscript', r_script])
    if result.returncode != 0:
        print("ERROR: R package requirements check failed (see above).")
        sys.exit(1)

_check_requirements()

# ── Mode flag ─────────────────────────────────────────────────────────────────
#
# Set BUILD_FROM_RAW = True to rebuild all panel datasets from raw input files.
# Leave as False to use the pre-built data files in ../data/ (default).

BUILD_FROM_RAW = True  # Set to False to skip panel-building scripts and use pre-built data files

_DATA_PREP = [
    # ── Data preparation (skipped when BUILD_FROM_RAW = False) ────────────────
    ('Build GP Patient Survey panel',         '../create_gp_patient_survey_panel/GPPS_MAIN.py'),
    ('Build GP practice registrations panel', '../create_gp_practice_registrations_panel/build_registrations_panel.py'),
    ('Build BES analysis panel',              '../create_BES_panel/build_bes_panel.py'),
]

SCRIPTS = (_DATA_PREP if BUILD_FROM_RAW else []) + [
    # ── Initial Figures ───────────────────────────────────────────────────────────────
    ('Figure 1',                           'fig1_NHS_SPENDING_Yougov.py'),
    ('Figure 2',                           'fig2_GP_closures_map.py'),
    ('Figure 3',                           'fig3_GP_closures_patients.py'),

    # ── GPPS analysis + figures ───────────────────────────────────────────────
    ('GPPS event-study models',            'gpps_analysis.R'),
    ('GPPS Honest DID',                    'honest_did_gpps.R'),
    ('GPPS event-study figures',           'fig4_gpps_event_study.py'),


    # ── BES analysis + figures ───────────────────────────────────────────────
    ('BES analysis',                       'bes_analysis.R'), # TEST_RUN = True for quick run; False for full (~50+ hrs on 16-core)
    ('BES event-study figures',            'fig6_BES_event_study.py'),

    # ── Text analysis figures ───────────────────────────────────────────────
    ('Text analysis figures',              'fig7_text_analysis.py'),

    # ── Moderation analysis ───────────────────────────────────────────────
    ('Moderation analysis',                'bes_moderation_analysis.R'),

    # ── Appendix figures ───────────────────────────────────────────────────────────────
    ('Figure A1',                          'figA1_GP_closures_heatmap.py'),
    ('Figure A2',                          'figA2_GP_closures_choropleth.py'),
    ('Figures A18–A19 (BES validation)',   'figA18_A19_bes_validation.py'),
]

script_dir = os.path.dirname(os.path.abspath(__file__))
results = []

for label, script in SCRIPTS:
    path = os.path.join(script_dir, script)
    print(f"\n{'='*60}")
    print(f"  Running {label}: {script}")
    print(f"{'='*60}")
    cmd = ['Rscript', path] if script.endswith('.R') else [sys.executable, path]
    result = subprocess.run(cmd, cwd=script_dir)
    ok = result.returncode == 0
    results.append((label, script, ok))

print(f"\n{'='*60}")
print("  Summary")
print(f"{'='*60}")
for label, script, ok in results:
    status = 'OK' if ok else 'FAILED'
    print(f"  [{status:6s}]  {label}: {script}")

if not all(ok for _, _, ok in results):
    sys.exit(1)
