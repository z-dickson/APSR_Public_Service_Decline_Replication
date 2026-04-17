# Replication Package: Public Service Decline and Support for the Populist Right

**Zachary P. Dickson, Sara B. Hobolt, Catherine E. De Vries, and Simone Cremaschi**

*American Political Science Review* (forthcoming)

---

## Abstract

The rise of populist right parties is well-studied, but relatively little attention has been given to how public service performance influences voter support. Given that public services are often the primary means through which citizens interact with the state, we argue that declining public services can create grievances that increase the appeal of populist right parties. Focusing on England’s National Health Service (NHS), we combine administrative data on local health facility closures with panel data on public preferences and voting intentions. Using a staggered difference-in-differences design, we find that closures reduce satisfaction with public services and increase support for populist right parties. These effects are moderated by migrant registrations at local health practices, highlighting the interaction between public service performance and immigration concerns in fueling populist right support. Our findings underscore the role of public service decline as a driver of support for populist parties, especially in areas undergoing demographic change.

---


# Replication Instructions

Replication materials for this article require R (version 4.2+) and Python (version 3.11 or 3.12). All scripts are designed to run on a standard desktop or laptop computer. Several datasets require the user to first obtain access through the UK Data Service; instructions for obtaining these files are provided below. All other data files are included in the `data/` folder of this repository.

## Requirements

**Python environment:** We recommend using the provided `environment.yml` to create a conda environment with the correct Python version (3.11) and all required packages:

```bash
conda env create -f environment.yml
conda activate nhs_replication
python -m spacy download en_core_web_sm
```

Python 3.13+ is **not** supported — several compiled dependencies do not yet have pre-built wheels for those versions.

**R packages:** R packages are checked and installed automatically the first time `main.py` is run (via `requirements.R`). This requires an internet connection on the first run. `HonestDiD` is installed from GitHub; all other packages are available on CRAN.



# Quick Start
1. Create the conda environment and install R packages as described above.
2. Run `python code/main.py` from the root of the repository. This will execute all scripts in the replication pipeline using the pre-built data files included in the `data/` folder. If you want to build the analysis panels from raw data instead, see the instructions in the sections below and set `BUILD_FROM_RAW = True` in `code/main.py`.

---

## Repository Structure

```
├── data/                                   # Input data files (see Data Sources below)
├── code/                                   # Analysis and figure scripts
│   └── main.py                             # Master script — runs the full pipeline
├── create_gp_patient_survey_panel/         # Builds data/gp_patient_survey_panel.parquet
│   ├── GPPS_MAIN.py                        # Master script for this sub-pipeline
│   ├── create_gp_panel.py                  # Step 1: stack raw annual CSVs into gp_panel.csv
│   ├── add_treatment_indicator.py          # Step 2: add treated/gvar columns
│   ├── add_covariates_to_GPPS.py          # Step 3: add outcomes and covariates; save parquet
│   ├── get_practice_closures.py            # Helper: parse epraccur and attach postcodes
│   ├── merge_immigration_statistics.py     # Helper: immigration covariate merges
│   ├── IMD_panel.py                        # Helper: IMD interpolation and merge
│   └── GP patient survey/                  # Raw annual survey CSVs (2012–2023)
├── create_gp_practice_registrations_panel/ # Builds data/gp_practice_registrations_panel.csv
│   ├── build_registrations_panel.py        # Single script: read, merge, derive, save
│   └── gp practice registrations/          # Raw annual patient count CSVs (2013–2022)
├── create_BES_panel/                       # Builds data/bes_analysis.parquet
│   ├── build_bes_panel.py                  # Single script: wide→long, covariates, save
│   ├── msoa_lookup.csv                     # MSOA11→MSOA21CD→LAD22CD lookup
│   ├── merge_immigration_statistics.py     # Helper: immigration covariate merges
│   ├── IMD_panel.py                        # Helper: IMD interpolation and merge
│   └── bes_panel_ukds_w1w25_v1.tab        # Raw BES wide-format file (from UK Data Service)
├── output_data_for_figures/                # Intermediate CSVs from R analysis scripts
└── final_output_for_article/              # Figures (PNG) and tables (TEX) — created on run
```

---

## Replication

To reproduce all figures and tables, run from the `code/` directory:

```bash
python main.py
```

All files to reproduce the main analyses (except the USOC analysis) are included in this repository, so this will run without any additional setup. However, if you want to build the analysis panels from raw data files (instead of using the pre-built, anonymized files included in `data/`), you will need to first obtain the raw data files as described in the sections below and then set `BUILD_FROM_RAW = True` in `main.py` (see Notes below for details).

Scripts are executed in order. The first three steps build panel datasets from raw data (see sections below); the remaining steps produce figures and tables. R scripts are run via `Rscript`; Python scripts via the current interpreter. See `main.py` for the full list of active scripts and comments on how to enable optional/appendix outputs.

**Note on BES and GP analysis:** `bes_analysis.R` and `gpps_analysis.R` include a `TEST_RUN` flag (default `TRUE`) that skips the matrix completion model, which can take several hours even on a 16-core machine. Set `TEST_RUN <- FALSE` to run the full analysis, which will produce the final coefficient CSVs used for plotting. The `TEST_RUN` option still runs the Sun & Abraham event-study models and produces regression tables, so it can be used for quick checks of the code.

**Note on creating the panel datasets:** The three panel datasets (`gp_patient_survey_panel.parquet`, `gp_practice_registrations_panel.csv`, and `bes_analysis.parquet`) are all built from raw data by scripts in the `create_gp_patient_survey_panel/`, `create_gp_practice_registrations_panel/`, and `create_BES_panel/` folders, respectively. These scripts are called automatically as the first steps of `main.py` if the `BUILD_FROM_RAW` flag is set to `True`. If you want to skip the panel-building step and use the pre-built data files included in the `data/` folder, set `BUILD_FROM_RAW = False` (the default). These data files need to be downloaded separately by the user. Instructions on where to find them is provided in the sections below. 



---

## GP Patient Survey panel

`data/gp_patient_survey_panel.parquet` is constructed from raw data by `create_gp_patient_survey_panel/GPPS_MAIN.py`, which is called automatically as the first step of `code/main.py`. It can also be run independently:

```bash
python create_gp_patient_survey_panel/GPPS_MAIN.py
```

### Downloading the raw survey files

Before running the pipeline, download the practice-level results CSV for each survey year (2012–2023) from the GP Patient Survey website: [https://gp-patient.co.uk/surveys-and-reports](https://gp-patient.co.uk/surveys-and-reports)

For each year, navigate to that year's report page and download the file labelled **"Practice-level data"** (or equivalent — the filename format varies by year). Save each file into the following folder using the year as the filename:

```
create_gp_patient_survey_panel/
└── GP patient survey/
    ├── 2012.csv
    ├── 2013.csv
    ├── 2014.csv
    ├── 2015.csv
    ├── 2016.csv
    ├── 2017.csv
    ├── 2018.csv
    ├── 2019.csv
    ├── 2020.csv
    ├── 2021.csv
    ├── 2022.csv
    └── 2023.csv
```

The `variable_lists/` subfolder (already included in the repository) contains one CSV per year that maps variable names to question descriptions; these do not need to be downloaded.

### Pipeline steps

| Step | Script | Description |
|---|---|---|
| 1 | `create_gp_panel.py` | Loads raw annual CSVs from `GP patient survey/` (2012–2023) and stacks them into a single panel indexed by `(practice_code, year)`. Saves `GP patient survey/gp_panel.csv`. |
| 2 | `add_treatment_indicator.py` | Merges closure dates from `epraccur.csv` and postcode–MSOA lookups from `postcodes_2023.parquet` to add `treated`, `treated_nearest`, `gvar`, `gvar_nearest`, and `dist_nearest_closed`. Updates `gp_panel.csv` in place. |
| 3 | `add_covariates_to_GPPS.py` | Creates outcome variables, merges immigration, unemployment, GDI, and IMD covariates, and saves the analysis-ready file to `data/gp_patient_survey_panel.parquet`. |

---

## GP practice registrations panel

`data/gp_practice_registrations_panel.csv` is constructed from raw data by `create_gp_practice_registrations_panel/build_registrations_panel.py`, which is called automatically as the second data preparation step of `code/main.py`. It can also be run independently:

```bash
python create_gp_practice_registrations_panel/build_registrations_panel.py
```

### Downloading the raw registration files

Annual patient registration counts are published by NHS Digital each December. Download the practice-level patient list size file for each year from:

[https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice](https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice)

For each year, find the December publication, download the practice-level CSV, and save it with the filename `gp_reg_YYYY.csv` in the folder below:

```
create_gp_practice_registrations_panel/
└── gp practice registrations/
    ├── gp_reg_2013.csv
    ├── gp_reg_2014.csv
    ├── gp_reg_2015.csv
    ├── gp_reg_2016.csv
    ├── gp_reg_2017.csv
    ├── gp_reg_2018.csv
    ├── gp_reg_2019.csv
    ├── gp_reg_2020.csv
    ├── gp_reg_2021.csv
    └── gp_reg_2022.csv
```

**Note on file formats:** The raw files changed format around 2017. Files for 2013–2016 are wide-format CSVs with a `TOTAL_ALL` column; files for 2017–2022 are long-format CSVs requiring a filter on `SEX=ALL` and `AGE=ALL`. The build script handles both formats automatically.

---

## BES analysis panel

`data/bes_analysis.parquet` is constructed from raw data by `create_BES_panel/build_bes_panel.py`, which is called automatically as the third data preparation step of `code/main.py`. It can also be run independently:

```bash
python create_BES_panel/build_bes_panel.py
```

### Downloading the raw BES file

The British Election Study Internet Panel (Waves 1–25) must be obtained from the UK Data Service:

1. Register for an account at [https://ukdataservice.ac.uk](https://ukdataservice.ac.uk)
2. Search for **SN 8202** (British Election Study Internet Panel, 2014–2023) and apply for access
3. Download the SPSS/tab-delimited version of the file; the relevant file is named `bes_panel_ukds_w1w25_v1.tab`
4. Save it into the following folder:

```
create_BES_panel/
└── bes_panel_ukds_w1w25_v1.tab
```

The script also requires `data/ons_data_all.xlsx` (ONS local authority covariates), which is already provided in the `data/` folder. The data can be downloaded directly from the ONS website [here](https://www.ons.gov.uk/explore-local-statistics/insights/datadownload.ods) and then saved to the `data/` folder.

### Pipeline steps

The script performs two main steps in sequence:

| Step | Description |
|---|---|
| 1 | Reads the wide-format BES tab file and converts it to a long panel indexed by `(id, wave)`. Adds MSOA21CD and LAD22CD from `msoa_lookup.csv`. |
| 2 | Merges GP closure treatment variables (from `epraccur.csv`), outcome variables, ONS local authority covariates (`ons_data_all.xlsx`), immigration covariates, and IMD deprivation scores. Filters to English respondents and saves `data/bes_analysis.parquet`. |

---

## Scripts

| Script | Output |
|---|---|
| `../create_gp_patient_survey_panel/GPPS_MAIN.py` | `data/gp_patient_survey_panel.parquet` — GP Patient Survey panel (2012–2023) |
| `../create_gp_practice_registrations_panel/build_registrations_panel.py` | `data/gp_practice_registrations_panel.csv` — practice registration counts (2013–2022) |
| `../create_BES_panel/build_bes_panel.py` | `data/bes_analysis.parquet` — BES long panel with treatment indicators and covariates (Waves 1–25) |
| `fig1_NHS_SPENDING_Yougov.py` | Figure 1: YouGov NHS spending tracker |
| `fig2_GP_closures_map.py` | Figure 2: Map of GP practice closures (2013–2023) |
| `fig3_GP_closures_patients.py` | Figure 3: Patients affected and closures per year |
| `gpps_analysis.R` | GPPS Sun & Abraham event-study models; coefficient CSVs and regression tables |
| `honest_did_gpps.R` | Rambachan & Roth (2023) sensitivity analysis for GPPS models |
| `fig4_gpps_event_study.py` | Figure 4: GPPS event-study coefficient plots |
| `bes_analysis.R` | BES Sun & Abraham event-study models; coefficient CSVs and regression tables |
| `fig6_BES_event_study.py` | Figures 6, 9, A9, A13, A14, A16: BES event-study coefficient plots |
| `fig7_text_analysis.py` | Figure 7: NHS–immigration linkage rates in UKIP/Reform UK communications |
| `bes_moderation_analysis.R` | Figure 8 and appendix: treatment effect heterogeneity (binning estimator) |
| `figA1_GP_closures_heatmap.py` | Figure A1: Heatmap of GP closure locations |
| `figA2_GP_closures_choropleth.py` | Figure A2: GP closures per 10,000 population by local authority |
| `figA18_A19_bes_validation.py` | Figures A18–A19: BES vote intention vs actual vote share (requires `BES2024_W29_Panel_v29.1.dta`) |
| `anonymize_data.py` | **Package preparation only — not part of the replication pipeline.** Strips each of the three panel files down to the columns used in the analysis and replaces geographic identifiers (MSOA codes, LAD codes, practice codes) with sorted integers so that fixed-effects estimators still work but individual locations cannot be identified. The transformation is one-way: original geographic codes cannot be recovered from the integer encodings. |
| `../delete_regenerable_files.py` | **Utility — not part of the replication pipeline.** Deletes all pipeline-generated files (built panel parquets/CSVs, intermediate GPPS build files, and all contents of `output_data_for_figures/` and `final_output_for_article/`) so that a clean from-scratch re-run can be tested. Raw input data files are never touched. Run from the repo root: `python delete_regenerable_files.py`. |

---

## Data Sources

All data files are provided in the `data/` folder, except the individual surveys for the GPPS, the raw British Election Study and Understanding Society files, which must be obtained directly from the UK Data Service (see below). We are unable to share the USOC data or related code due to data sharing restrictions, but we have provided an anonymized version of the BES panel file (`bes_analysis.parquet`) and the GPPS panel file (`gp_patient_survey_panel.parquet`), both of which can be used to reproduce the main analyses. You can also run the full pipeline to build these files from the raw data if you have access to the UK Data Service and the GPPS survey files. The `bes_analysis.parquet` file includes all covariates derived from the BES raw data, while the `gp_patient_survey_panel.parquet` file includes all covariates derived from the GPPS raw data and the practice-level treatment indicators. The scripts for building these files are included in the repository, so you can modify the covariate construction or add new variables if desired.

| File | Description | Source |
|---|---|---|
| `gp_patient_survey_panel.parquet` | GP Patient Survey panel (2012–2023), built by `GPPS_MAIN.py` | [Ipsos MORI / NHS](https://gp-patient.co.uk/) |
| `bes_analysis.parquet` | British Election Study panel (Waves 1–25), built by `build_bes_panel.py` | [UK Data Service (SN 8202)](https://datacatalogue.ukdataservice.ac.uk/studies/study/8202#details) |
| `ons_data_all.xlsx` | ONS local authority covariates (inactivity, employment, claimant count, GDI, weekly pay, GDP, housing) | [ONS](https://www.ons.gov.uk/explore-local-statistics/) |
| `epraccur.csv` | NHS GP practice register including closure dates | [NHS Digital](https://digital.nhs.uk/services/organisation-data-service/data-search-and-export/csv-downloads/gp-and-gp-practice-related-data) |
| `postcodes_2023.parquet` | ONS postcode directory linking postcodes to MSOA/LAD geographies | [ONS](https://geoportal.statistics.gov.uk/datasets/bd25c421196b4546a7830e95ecdd70bc/about) |
| `iod_2010.parquet` | Index of Multiple Deprivation 2010 (LSOA-level scores) | [MHCLG](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2010) |
| `iod_2015.parquet` | Index of Multiple Deprivation 2015 (LSOA-level scores) | [MHCLG](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2015) |
| `iod_2019.parquet` | Index of Multiple Deprivation 2019 (LSOA-level scores) | [MHCLG](https://www.gov.uk/government/statistics/english-indices-of-deprivation-2019) |
| `UK_GP_registrations_of_migrants_per_local_authority.xlsx` | Migrant GP registrations, NiNo registrations, and migration flows by LAD | [ONS](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/migrationwithintheuk/datasets/localareamigrationindicatorsunitedkingdom) |
| `modelled-unemployment-table-data.csv` | Modelled unemployment rate by local authority | [ONS / NOMIS](https://www.nomisweb.co.uk/) |
| `BES2024_W29_Panel_v29.1.dta` | British Election Study full panel (Waves 1–29), used only for Figures A18–A19. The W1–25 UK Data Service file used for the main analysis (which includes the MSOA codes we use for the analysis) does not extend to the 2024 general election; the W29 panel is used here so the vote intention validation can include the most recent election. | [BES website](https://www.britishelectionstudy.com/data-objects/panel-study-data/) |
| `gp_closures_coords.csv` | GP practice closure coordinates (derived from epraccur) | Derived |
| `gp_practice_registrations_panel.csv` | Annual patient registration counts per GP practice (2013–2022), built by `build_registrations_panel.py` | [NHS Digital](https://digital.nhs.uk/data-and-information/publications/statistical/patients-registered-at-a-gp-practice) |
| `what-sector-should-the-uk-government-spend-more-on.xlsx` | YouGov NHS spending tracker | [YouGov](https://yougov.co.uk/topics/society/trackers/what-sector-should-the-uk-government-spend-more-on) |
| `ukip_videos_with_meta.csv` | UKIP YouTube video transcripts and metadata (2011–) | YouTube Data API |
| `reform_videos_with_meta.csv` | Reform UK YouTube video transcripts and metadata (2021–) | YouTube Data API |
| `ukip_press_releases.parquet` | UKIP press releases (2010–2024) | UKIP website |
| `Local_Authority_Districts_...geojson` | LAD boundary file (May 2024) | [ONS Open Geography Portal](https://geoportal.statistics.gov.uk/) |
| `population-count-table-data.csv` | Population counts by local authority | [ONS](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates) |

**Note on Understanding Society and BES data:** These files must be obtained separately from the UK Data Service and are not included in this replication package due to data sharing restrictions (see Notes below).

- Understanding Society: Waves 1-15, 2009-2024 and Harmonised BHPS: Waves 1-18, 1991-2009: Secure Access (Waves 1–13, SN 6676): https://datacatalogue.ukdataservice.ac.uk/studies/study/6676#details

---

## Notes

- The BES `bes_analysis.parquet` file is built from `create_BES_panel/build_bes_panel.py`. The raw input (`bes_panel_ukds_w1w25_v1.tab`) must be obtained from the UK Data Service (SN 8202) under a data sharing agreement, because the derived parquet contains MSOA identifiers. To obtain the raw file, register for an account with the UK Data Service and request access to the British Election Study (SN 8202).
- **USOC analysis:** We are unable to share the Understanding Society (USOC) data or the code for the USOC analysis due to data sharing agreements with the UK Data Service. This affects the following outputs: Figure 5, Figures A7–A8, A15, A17, and Tables A7–A8. These figures and tables cannot be reproduced from the materials in this repository. Researchers who wish to replicate this part of the analysis must obtain their own access to Understanding Society (SN 6676) from the UK Data Service at [https://datacatalogue.ukdataservice.ac.uk/studies/study/6676#details](https://datacatalogue.ukdataservice.ac.uk/studies/study/6676#details).
- `figA2_GP_closures_choropleth.py` requires `population-count-table-data.csv` in the `data/` folder. This file can be obtained from the ONS website as linked above.
- The `gp_closures_coords.csv` file is derived from `epraccur.csv` and is included in the `data/` folder for convenience. The script `get_practice_closures.py` in the `create_gp_patient_survey_panel/` folder shows how this file was created by parsing closure dates and attaching geocodes from the ONS postcode directory.
- The scripts for building the analysis panels (`build_bes_panel.py`, `GPPS_MAIN.py`, and `build_registrations_panel.py`) are designed to be run independently if the user only wants to reproduce one part of the analysis. However, they are also called in sequence by `code/main.py` to produce all final datasets and outputs in one run.
- The Index of Multiple Deprivation (IMD) scores are provided at the LSOA level, but our analysis is at the MSOA/LAD level. The `IMD_panel.py` script in both the `create_gp_patient_survey_panel/` and `create_BES_panel/` folders performs interpolation to create MSOA/LAD-level IMD covariates by averaging the LSOA scores weighted by population. This allows us to include a measure of local deprivation in our models without losing observations due to missing IMD data at higher geographic levels. Note that the data saved in the `data` folder are .parquet files instead of the original formats, so the IMD interpolation step is only needed if the user wants to modify the covariate construction or use the raw IMD scores for a different analysis.




---

## Codebook

Variable definitions for the three analysis-ready panel files included in `data/`. Geographic identifiers have been anonymized to sorted integers by `anonymize_data.py` (see the Scripts table above); all other values are unchanged.

---

### `bes_analysis.parquet` — British Election Study panel (Waves 1–25)

Unit of observation: respondent–wave.

| Variable | Type | Description |
|---|---|---|
| `id` | integer | Respondent identifier. Consistent across waves. |
| `wave` | integer | BES wave number (1–25). Used as the time index in the matrix completion model. |
| `year` | integer | Calendar year of the wave. Used as the time index in Sun & Abraham models. |
| `msoa11` | integer | Anonymized MSOA 2011 identifier for the respondent's area. Used as a fixed effect. |
| `gvar` | integer | Cohort variable: the first calendar year in which a GP practice closed within the respondent's MSOA. Respondents in MSOAs with no closure during the panel window are assigned `gvar = 10000` (never-treated). |
| `treatment` | integer | Binary treatment indicator: 1 if the respondent's MSOA has experienced at least one GP closure by wave `t`, 0 otherwise. Used as the treatment variable in the matrix completion model. |
| `open_after_close` | integer | Robustness flag: 1 if the respondent's MSOA had a new practice open shortly after a closure (i.e., temporary disruption). Used to construct alternative treatment assignments in the open-after-close robustness checks. |
| `rrw_vote` | float | **Primary outcome.** Intention to vote for a right-wing/populist right party (UKIP or Reform UK). Derived from BES vote intention questions. |
| `past_vote_rrw` | float | **Robustness outcome.** Recalled vote for a right-wing/populist right party at the most recent general election. |
| `conservative_vote` | float | Intention to vote Conservative. Used in mainstream party robustness checks. |
| `labour_vote` | float | Intention to vote Labour. Used in mainstream party robustness checks. |
| `libdem_vote` | float | Intention to vote Liberal Democrat. Used in mainstream party robustness checks. |
| `green_vote` | float | Intention to vote Green. Used in mainstream party robustness checks. |
| `IMD_Score` | float | Index of Multiple Deprivation score for the respondent's MSOA. Higher values indicate greater deprivation. Interpolated from 2010, 2015, and 2019 LSOA-level IMD scores. |
| `employment_rate` | float | Local authority employment rate (proportion of working-age population in employment). Source: ONS. |
| `international_migration_per_pop` | float | Long-term international migration inflow to the local authority as a proportion of population. Source: ONS Local Area Migration Indicators. |
| `migrant_gp_registrations_per_pop` | float | Number of new migrant GP registrations in the local authority as a proportion of population. Proxy for recent immigration pressure on local health services. Source: ONS. |

---

### `gp_patient_survey_panel.parquet` — GP Patient Survey panel (2012–2023)

Unit of observation: GP practice–year.

| Variable | Type | Description |
|---|---|---|
| `practice_code` | integer | Anonymized GP practice identifier. |
| `year` | integer | Survey year (2012–2023). |
| `oslaua` | integer | Anonymized Local Authority District (LAD) identifier. Used as a fixed effect in primary models. |
| `msoa21` | integer | Anonymized MSOA 2021 identifier for the practice's location. Used as a fixed effect in nearest-practice robustness models. |
| `gvar` | integer | Cohort variable: the first year in which any GP practice closed within the practice's MSOA. Practices in MSOAs with no panel-window closure are assigned `gvar = 10000` (never-treated). Used as the cohort variable in Sun & Abraham models. |
| `gvar_nearest` | integer | Cohort variable for the nearest-practice robustness specification: the year in which the geographically nearest GP practice closed. Practices with no nearby panel-window closure are assigned `gvar_nearest = 10000` (never-treated). |
| `treated` | integer | Binary treatment indicator: 1 if a GP practice has closed within the practice's MSOA by year `t`. |
| `treated_nearest` | integer | Binary treatment indicator for the nearest-practice specification: 1 if the geographically nearest GP practice has closed by year `t`. |
| `treatment` | integer | Binary treatment indicator used in the matrix completion model (equivalent to `treated`). |
| `positive_overall_experience_with_gp_practice` | float | **Primary outcome.** Practice-level proportion of patients reporting a positive overall experience with their GP practice. Derived from the GPPS "Overall, how would you describe your experience of your GP practice?" question. |
| `negative_overall_experience_making_an_appointment` | float | **Primary outcome.** Practice-level proportion of patients reporting a negative experience when making an appointment. Derived from the GPPS "Overall, how would you describe your experience of making an appointment?" question. |
| `IMD_Score` | float | Index of Multiple Deprivation score for the practice's MSOA. Interpolated from 2010, 2015, and 2019 LSOA-level IMD scores. |
| `unemployment_rate` | float | Local authority unemployment rate. Source: ONS/NOMIS modelled unemployment series. |
| `inflow_longterm_international_migration_proportion` | float | Long-term international migration inflow to the practice's local authority as a proportion of population. Source: ONS Local Area Migration Indicators. |
| `migrant_gp_registrations_proportion` | float | Migrant GP registrations in the practice's local authority as a proportion of all GP registrations. Source: ONS. |

---

### `gp_practice_registrations_panel.csv` — GP practice patient registration counts (2013–2022)

Unit of observation: GP practice–year. Used to compute patient displacement figures (Figure 3).

| Variable | Type | Description |
|---|---|---|
| `gp_practice_code` | integer | Anonymized NHS GP practice identifier. |
| `year` | integer | Year of the registration count (2013–2022). |
| `close_year` | integer | Year the practice closed. `NA` if the practice was still open at the end of the panel. |
| `patients_before_close` | integer | Number of patients registered at the practice in the year immediately before closure. Used to estimate the number of patients displaced by closures. |








> If you have any questions about the replication materials or encounter any issues running the code, please contact the lead author, Zachary P. Dickson (https://z-dickson.github.io/)