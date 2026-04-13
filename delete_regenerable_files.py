
# delete_regenerable_files.py
#
# Deletes all files produced by the replication pipeline so that a full
# from-scratch re-run can be tested.  Raw input data files are never touched.
#
# Files removed:
#   data/
#     gp_patient_survey_panel.parquet        ← GPPS_MAIN.py
#     gp_practice_registrations_panel.csv    ← build_registrations_panel.py
#     bes_analysis.parquet                   ← build_bes_panel.py
#     gp_closures_coords.csv                 ← get_practice_closures.py (via GPPS)
#
#   create_gp_patient_survey_panel/GP patient survey/
#     gp_panel.csv                           ← create_gp_panel.py
#     gp_panel_codebook.csv                  ← create_gp_panel.py
#
#   output_data_for_figures/  (all contents) ← R analysis scripts
#   final_output_for_article/ (all contents) ← figure scripts

import os
import glob

BASE = os.path.dirname(os.path.abspath(__file__))


def remove(path):
    if os.path.exists(path):
        os.remove(path)
        print(f"  deleted  {os.path.relpath(path, BASE)}")
    else:
        print(f"  missing  {os.path.relpath(path, BASE)}  (skipped)")


def remove_dir_contents(directory):
    files = glob.glob(os.path.join(directory, '*'))
    if not files:
        print(f"  empty    {os.path.relpath(directory, BASE)}/  (nothing to delete)")
        return
    for f in files:
        if os.path.isfile(f):
            os.remove(f)
            print(f"  deleted  {os.path.relpath(f, BASE)}")


print("=" * 60)
print("  Deleting pipeline-generated files")
print("=" * 60)

# ── Panel data files ───────────────────────────────────────────────────────────

print("\n── Panel data ──")
remove(os.path.join(BASE, "data", "gp_patient_survey_panel.parquet"))
remove(os.path.join(BASE, "data", "gp_practice_registrations_panel.csv"))
remove(os.path.join(BASE, "data", "bes_analysis.parquet"))
remove(os.path.join(BASE, "data", "gp_closures_coords.csv"))

# ── Intermediate GPPS build files ─────────────────────────────────────────────

print("\n── GPPS intermediate files ──")
gpps_dir = os.path.join(BASE, "create_gp_patient_survey_panel", "GP patient survey")
remove(os.path.join(gpps_dir, "gp_panel.csv"))
remove(os.path.join(gpps_dir, "gp_panel_codebook.csv"))

# ── Analysis output CSVs ───────────────────────────────────────────────────────

print("\n── output_data_for_figures/ ──")
remove_dir_contents(os.path.join(BASE, "output_data_for_figures"))

# ── Final article outputs ──────────────────────────────────────────────────────

print("\n── final_output_for_article/ ──")
remove_dir_contents(os.path.join(BASE, "final_output_for_article"))

print("\n" + "=" * 60)
print("  Done. Run main.py (with BUILD_FROM_RAW = True) to rebuild.")
print("=" * 60)
