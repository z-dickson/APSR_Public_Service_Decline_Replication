
# GP Patient Survey (GPPS) Analysis
# Sun & Abraham (2021) event-study models for GP patient satisfaction outcomes,
# with progressively richer control variable sets (mirroring the BES approach).
#
# Input:
#   ../data/gp_patient_survey_panel.parquet
#
# Outputs (CSVs → ../output_data_for_figures/, tables → ../final_output_for_article/):
#   ../output_data_for_figures/gpps_coefficient_estimates.csv
#   ../output_data_for_figures/gpps_nearest_coefficient_estimates.csv
#   ../final_output_for_article/gpps_positive_models.tex
#   ../final_output_for_article/gpps_negative_models.tex
#   ../final_output_for_article/gpps_nearest_positive_models.tex
#   ../final_output_for_article/gpps_nearest_negative_models.tex
#
#   Matrix completion ATT (slow; skipped when TEST_RUN = TRUE)
#     ../output_data_for_figures/gpps_negative_mc_att.csv
#     ../output_data_for_figures/gpps_positive_mc_att.csv

library(fect)
library(fixest)
library(nanoparquet)
library(dplyr)


# ── Configuration ─────────────────────────────────────────────────────────────
#
# Set TEST_RUN = TRUE to skip the matrix completion models, which require
# bootstrap resampling and can take hours on large data.  All Sun & Abraham
# event-study models are unaffected by this flag.

TEST_RUN <- TRUE


# ── Data ─────────────────────────────────────────────────────────────────────

GPPS_data <- read_parquet('../data/gp_patient_survey_panel.parquet')


OUTPUT_DIR  <- '../output_data_for_figures/'   # intermediate CSVs
ARTICLE_DIR <- '../final_output_for_article/'  # figures and tables
dir.create(OUTPUT_DIR,  showWarnings = FALSE, recursive = TRUE)
dir.create(ARTICLE_DIR, showWarnings = FALSE, recursive = TRUE)

# ── Models ────────────────────────────────────────────────────────────────────
#
# Model progression mirrors the BES primary analysis:
#   m1: no controls          | oslaua + year
#   m2: + IMD Score
#   m3: + IMD Score + Unemployment Rate
#   m4: + IMD Score + Unemployment Rate + migration controls
#
# Fixed effects: LAD (oslaua) + year.
# Standard errors: two-way clustered (LAD × year), via vcov = 'twoway'.

# ── Positive overall experience ───────────────────────────────────────────────

pos_m1 <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar, year) |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

pos_m2 <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar, year) + IMD_Score |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

pos_m3 <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar, year) +
    IMD_Score + unemployment_rate |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

pos_m4 <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar, year) +
    IMD_Score + unemployment_rate +
    inflow_longterm_international_migration_proportion +
    migrant_gp_registrations_proportion |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')



# ── Negative appointment experience ──────────────────────────────────────────

neg_m1 <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar, year) |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

neg_m2 <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar, year) + IMD_Score |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

neg_m3 <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar, year) +
    IMD_Score + unemployment_rate |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

neg_m4 <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar, year) +
    IMD_Score + unemployment_rate +
    inflow_longterm_international_migration_proportion +
    migrant_gp_registrations_proportion |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')



# ── Save event-study CSVs (full model, m4, for plotting) ─────────────────────
#
# Two outputs:
#
#   1. gpps_coefficient_estimates.csv — combined CSV used by clean_ggps_data()
#      in figures_functions.py. Only event-study rows (year::*) are included.
#      Positive rows have "1" appended to their row names so that the Python
#      parser (which calls str[:-1] on the positive subset) extracts the correct
#      event-time period.
#
#   2. Separate per-outcome CSVs for any other downstream use.

neg_es <- as.data.frame(neg_m1$coeftable)
neg_es <- neg_es[grepl("^year::", rownames(neg_es)), ]
neg_es$outcome <- 'negative_overall_experience_making_an_appointment'

pos_es <- as.data.frame(pos_m1$coeftable)
pos_es <- pos_es[grepl("^year::", rownames(pos_es)), ]
pos_es$outcome <- 'positive_overall_experience_with_gp_practice'
rownames(pos_es) <- paste0(rownames(pos_es), "1")   # expected by clean_ggps_data()

gpps_results <- rbind(neg_es, pos_es)
write.csv(gpps_results, paste0(OUTPUT_DIR, 'gpps_coefficient_estimates.csv'))


# ── Regression tables (ATT-aggregated) ───────────────────────────────────────
#
# The event-study spans many periods, so we aggregate to the ATT for the table.
# Event-study dynamics are shown in the figures.
# etable() is used (fixest-native) for clean LaTeX output with agg = "att".

var_dict <- c(
  'ATT'                                                = 'ATT',
  'IMD_Score'                                          = 'IMD Score',
  'unemployment_rate'                                  = 'Unemployment Rate',
  'inflow_longterm_international_migration_proportion' = 'International Migration (inflow)',
  'migrant_gp_registrations_proportion'                = 'Migrant GP Registrations',
  'oslaua'                                             = 'LAD',
  'year'                                               = 'Year'
)

gpps_notes <- "Two-way clustered (LAD $\\times$ year) standard errors in parentheses.
  All models include LAD and year fixed effects. ATT is the \\citet{sun2021estimating}
  aggregated average treatment effect on the treated.
  Model 1: no controls. Model 2: adds IMD Score. Model 3: adds Unemployment Rate.
  Model 4: adds migration controls (international migration inflow proportion and
  migrant GP registrations proportion)."

etable(
  list(pos_m1, pos_m2, pos_m3, pos_m4),
  agg    = "att",
  dict   = var_dict,
  title  = "Effects of GP Practice Closures on Positive Overall Experience (GPPS)",
  label  = "tab:gpps_positive_models",
  notes  = gpps_notes,
  tex    = TRUE,
  file   = paste0(ARTICLE_DIR, 'gpps_positive_models.tex')
)

etable(
  list(neg_m1, neg_m2, neg_m3, neg_m4),
  agg    = "att",
  dict   = var_dict,
  title  = "Effects of GP Practice Closures on Negative Appointment Experience (GPPS)",
  label  = "tab:gpps_negative_models",
  notes  = gpps_notes,
  tex    = TRUE,
  file   = paste0(ARTICLE_DIR, 'gpps_negative_models.tex')
)

cat("Done.\n")
cat("  Tables → gpps_{positive,negative}_models.tex')")
cat("  CSVs   → gpps_{positive,negative}_event_study.csv')")


# ── Nearest-practice robustness: gvar_nearest + msoa21 + year FE ─────────────
#
# Alternative treatment assignment: a practice is treated from the year in
# which its nearest GP practice closes (gvar_nearest), rather than any closure
# within the same MSOA (gvar).  Proximity is Euclidean distance on lat/long
# coordinates.  Practices with no panel-window closure nearby are assigned
# gvar_nearest = 10000 (never-treated).
#
# Fixed effects: MSOA (msoa21) + year — finer-grained than the LAD FE used
# in the primary models, exploiting within-MSOA variation.
# Standard errors: two-way clustered (msoa21 x year), via vcov = 'twoway'.
#
# Outputs:
#   results/gpps_nearest_positive_event_study.csv
#   results/gpps_nearest_negative_event_study.csv
#   article/tables/gpps_nearest_positive_models.tex
#   article/tables/gpps_nearest_negative_models.tex
#   article/figures/gpps_nearest_coefficient_estimates.png

# ── Positive outcome ──────────────────────────────────────────────────────────

nearest_pos_m1 <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar_nearest, year) |
  msoa21 + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

nearest_pos_m2 <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar_nearest, year) +
    IMD_Score |
  msoa21 + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

nearest_pos_m3 <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar_nearest, year) +
    IMD_Score + unemployment_rate |
  msoa21 + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

nearest_pos_m4 <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar_nearest, year) +
    IMD_Score + unemployment_rate +
    inflow_longterm_international_migration_proportion +
    migrant_gp_registrations_proportion |
  msoa21 + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')


# ── Negative outcome ──────────────────────────────────────────────────────────

nearest_neg_m1 <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar_nearest, year) |
  msoa21 + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

nearest_neg_m2 <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar_nearest, year) +
    IMD_Score |
  msoa21 + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

nearest_neg_m3 <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar_nearest, year) +
    IMD_Score + unemployment_rate |
  msoa21 + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

nearest_neg_m4 <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar_nearest, year) +
    IMD_Score + unemployment_rate +
    inflow_longterm_international_migration_proportion +
    migrant_gp_registrations_proportion |
  msoa21 + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')


# ── Save combined event-study CSV (same format as gpps_coefficient_estimates.csv)
#
# The Python figures pipeline (clean_ggps_data / plot_gpps_coefficient_estimates)
# expects a combined CSV where:
#   - negative rows have row names  year::-5, year::-4, …
#   - positive rows have row names  year::-51, year::-41, … (trailing "1")
# This mirrors exactly how gpps_coefficient_estimates.csv is produced above.

nearest_neg_es <- as.data.frame(nearest_neg_m1$coeftable)
nearest_neg_es <- nearest_neg_es[grepl("^year::", rownames(nearest_neg_es)), ]
nearest_neg_es$outcome <- "negative_overall_experience_making_an_appointment"

nearest_pos_es <- as.data.frame(nearest_pos_m1$coeftable)
nearest_pos_es <- nearest_pos_es[grepl("^year::", rownames(nearest_pos_es)), ]
nearest_pos_es$outcome <- "positive_overall_experience_with_gp_practice"
rownames(nearest_pos_es) <- paste0(rownames(nearest_pos_es), "1")

nearest_combined <- rbind(nearest_neg_es, nearest_pos_es)
write.csv(nearest_combined, paste0(OUTPUT_DIR, 'gpps_nearest_coefficient_estimates.csv'))


# ── Regression tables (ATT-aggregated) ───────────────────────────────────────

nearest_var_dict <- c(
  'ATT'                                                = 'ATT',
  'IMD_Score'                                          = 'IMD Score',
  'unemployment_rate'                                  = 'Unemployment Rate',
  'inflow_longterm_international_migration_proportion' = 'International Migration (inflow)',
  'migrant_gp_registrations_proportion'                = 'Migrant GP Registrations',
  'msoa21'                                             = 'MSOA',
  'year'                                               = 'Year'
)

nearest_notes <- "Two-way clustered (MSOA $\\times$ year) standard errors in parentheses.
  All models include MSOA and year fixed effects. ATT is the \\citet{sun2021estimating}
  aggregated average treatment effect on the treated. Treatment is assigned from the
  year in which the nearest GP practice closes (\\texttt{gvar\\_nearest}).
  Model 1: no controls. Model 2: adds IMD Score. Model 3: adds Unemployment Rate.
  Model 4: adds migration controls (international migration inflow proportion and
  migrant GP registrations proportion)."

etable(
  list(nearest_pos_m1, nearest_pos_m2, nearest_pos_m3, nearest_pos_m4),
  agg    = "att",
  dict   = nearest_var_dict,
  title  = "Nearest-Practice Treatment: Effects on Positive Overall Experience (GPPS)",
  label  = "tab:gpps_nearest_positive_models",
  notes  = nearest_notes,
  tex    = TRUE,
  file   = paste0(ARTICLE_DIR, 'gpps_nearest_positive_models.tex')
)

etable(
  list(nearest_neg_m1, nearest_neg_m2, nearest_neg_m3, nearest_neg_m4),
  agg    = "att",
  dict   = nearest_var_dict,
  title  = "Nearest-Practice Treatment: Effects on Negative Appointment Experience (GPPS)",
  label  = "tab:gpps_nearest_negative_models",
  notes  = nearest_notes,
  tex    = TRUE,
  file   = paste0(ARTICLE_DIR, 'gpps_nearest_negative_models.tex')
)

cat("Nearest-practice models done.\n")
cat("  Combined CSV → gpps_nearest_coefficient_estimates.csv")
cat("  Tables       → gpps_nearest_{positive,negative}_models.tex")









# ── Matrix completion ATT  [slow — skipped when TEST_RUN = TRUE] ──────────────
#
# Robustness check using the interactive fixed-effects / matrix completion
# estimator (Athey et al. 2021) from the fect package.  Unlike Sun & Abraham
# (2021), this approach imputes counterfactual outcomes via low-rank matrix
# factorisation rather than a two-way FE model.
#
# Indexed at the practice level (practice_code × year).
# One model per outcome, each with unemployment_rate as a covariate.

if (!TEST_RUN) {

  gpps_mc_neg <- fect(
    negative_overall_experience_making_an_appointment ~ treatment + unemployment_rate,
    data         = GPPS_data,
    method       = "fe",
    index        = c("practice_code", "year"),
    r            = 5,
    k            = 5,
    min.T0       = 1,
    na.rm        = FALSE,
    CV           = TRUE,
    cv.treat     = FALSE,
    force        = "two-way",
    fill.missing = TRUE,
    se           = TRUE,
    parallel     = TRUE,
    nboots       = 100
  )

  gpps_mc_pos <- fect(
    positive_overall_experience_with_gp_practice ~ treatment + unemployment_rate,
    data         = GPPS_data,
    method       = "fe",
    index        = c("practice_code", "year"),
    r            = 5,
    k            = 5,
    min.T0       = 1,
    na.rm        = FALSE,
    CV           = TRUE,
    cv.treat     = FALSE,
    force        = "two-way",
    fill.missing = TRUE,
    se           = TRUE,
    parallel     = TRUE,
    nboots       = 100
  )

  write.csv(data.frame(gpps_mc_neg$est.att),
            paste0(OUTPUT_DIR, 'gpps_negative_mc_att.csv'))
  write.csv(data.frame(gpps_mc_pos$est.att),
            paste0(OUTPUT_DIR, 'gpps_positive_mc_att.csv'))

  cat("Matrix completion done.\n")
  cat("  CSVs → gpps_{negative,positive}_mc_att.csv')")

} else {
  cat("TEST_RUN = TRUE: matrix completion skipped.\n")
}
