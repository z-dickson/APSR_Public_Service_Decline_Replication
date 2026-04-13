# BES Analysis
# Sun & Abraham (2021) event-study models for right-wing vote intention using
# British Election Study (BES) panel data.  Respondents are linked to their
# MSOA, allowing treatment to be assigned at the MSOA level (year a GP practice
# first closes within the MSOA).
#
# Input:
#   ../data/bes_analysis.parquet - this version of the dataset is available from the UK Data Service (it is protected because it contains MSOA identifiers, but researchers can apply for access to the full version with a data sharing agreement).
#
# Outputs (all → output_data_for_figures/):
#
#   Primary analysis
#     bes_primary_m1.csv  – no controls
#     bes_primary_m2.csv  – + IMD Score
#     bes_primary_m3.csv  – + IMD Score + employment rate
#     bes_primary_m4.csv  – + IMD Score + employment rate + migration controls
#     bes_event_study_vote_intention.tex
#
#   Robustness: past vote
#     bes_robust_past_vote.csv
#
#   Robustness: not-yet-treated control group
#     bes_robust_not_yet_treated.csv
#
#   Robustness: mainstream party vote intentions
#     bes_robust_<party>.csv          – no controls
#     bes_robust_controls_<party>.csv – with controls
#     bes_robust_mainstream_parties.tex
#     bes_robust_mainstream_parties_controls.tex
#
#   Robustness: open-after-close treatment assignment
#     bes_robust_open_after_close_as_treated.csv
#     bes_robust_open_after_close_as_controls.csv
#     bes_robust_open_after_close.tex
#
#   Matrix completion ATT (slow; skipped when TEST_RUN = TRUE)
#     bes_matrix_completion_att.csv

library(fect)
library(nanoparquet)
library(fixest)
library(dplyr)

OUTPUT_DIR  <- '../output_data_for_figures/'   # intermediate CSVs
ARTICLE_DIR <- '../final_output_for_article/'  # figures and tables
dir.create(OUTPUT_DIR,  showWarnings = FALSE, recursive = TRUE)
dir.create(ARTICLE_DIR, showWarnings = FALSE, recursive = TRUE)


# ── Configuration ─────────────────────────────────────────────────────────────
#
# Set TEST_RUN = TRUE to skip the matrix completion model, which requires
# bootstrap resampling and can take hours on large data.  All other models
# run in seconds to minutes and are unaffected by this flag.

TEST_RUN <- TRUE


# ── Data ──────────────────────────────────────────────────────────────────────

bes <- read_parquet('../data/bes_analysis.parquet')


# ── Helper ────────────────────────────────────────────────────────────────────
#
# Extract the full coefficient table from a fixest model and tag it with the
# outcome variable name before writing to CSV.

save_coeftable <- function(model, outcome_name, file_path) {
  out <- as.data.frame(model$coeftable)
  out$outcome <- outcome_name
  write.csv(out, file_path)
}


# ── Variable dictionary (shared across tables) ────────────────────────────────

var_dict <- c(
  'ATT'                                    = 'ATT',
  'IMD_Score'                              = 'IMD Score',
  'employment_rate'                        = 'Employment Rate',
  'international_migration_per_pop'        = 'International Migration',
  'migrant_gp_registrations_per_pop'       = 'Migrant GP Registrations',
  'id'                                     = 'Respondent',
  'msoa11'                                 = 'MSOA',
  'year'                                   = 'Year'
)


# ══════════════════════════════════════════════════════════════════════════════
# 1. PRIMARY ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
#
# Outcome: rrw_vote (right-wing/Reform vote intention)
# Estimator: Sun & Abraham (2021) via sunab(gvar, year)
# Fixed effects: respondent (id) + year + MSOA (msoa11)
# SEs: two-way clustered (id × year)
#
# gvar is the cohort variable: the first year of treatment (GP closure) in the
# respondent's MSOA.  Never-treated MSOAs have gvar = 10000.
#
# Model progression adds controls sequentially to assess sensitivity:
#   m1 – no controls
#   m2 – area deprivation (IMD Score)
#   m3 – + employment rate
#   m4 – + international migration and migrant GP registrations
#
# Year range is capped at < 2023 because analysis ends (migration controls unavailable from 2023 onward).  This restriction applies to all models in the progression for comparability.

bes_primary <- bes[bes$year < 2023, ]

m1 <- feols(rrw_vote ~ sunab(gvar, year) |
              id + year + msoa11,
            bes_primary, nthreads = 12, cluster = ~id + year)

m2 <- feols(rrw_vote ~ sunab(gvar, year) + IMD_Score |
              id + year + msoa11,
            bes_primary, nthreads = 12, cluster = ~id + year)

m3 <- feols(rrw_vote ~ sunab(gvar, year) + IMD_Score + employment_rate |
              id + year + msoa11,
            bes_primary, nthreads = 12, cluster = ~id + year)

m4 <- feols(rrw_vote ~ sunab(gvar, year) + IMD_Score + employment_rate +
              international_migration_per_pop + migrant_gp_registrations_per_pop |
              id + year + msoa11,
            bes_primary, nthreads = 12, cluster = ~id + year)


# Save per-model CSVs (event-study coefficients used by the figures pipeline)
primary_models <- list(m1, m2, m3, m4)
for (i in seq_along(primary_models)) {
  save_coeftable(
    primary_models[[i]],
    outcome_name = paste0("rrw_vote_m", i),
    file_path    = paste0(OUTPUT_DIR, 'bes_primary_m', i, '.csv')
  )
}

# Regression table (ATT-aggregated; event-study dynamics are shown in figures)
primary_notes <- "Two-way clustered (respondent $\\times$ year) standard errors in parentheses.
  All models include respondent, year, and MSOA fixed effects. ATT is the
  \\citet{sun2021estimating} aggregated average treatment effect on the treated.
  Model 1: no controls. Model 2: adds IMD Score. Model 3: adds Employment Rate.
  Model 4: adds migration controls (international migration and migrant GP
  registrations). Sample restricted to 2013--2022 (migration data unavailable
  from 2023)."

etable(
  list(m1, m2, m3, m4),
  agg   = "att",
  dict  = var_dict,
  title = "Effects of GP Practice Closures on Right-Wing Vote Intentions (BES)",
  label = "tab:bes_event_study_vote_intention",
  notes = primary_notes,
  tex   = TRUE,
  file  = paste0(ARTICLE_DIR, "bes_event_study_vote_intention.tex")
)

cat("Primary analysis done.\n")


# ══════════════════════════════════════════════════════════════════════════════
# 2. ROBUSTNESS: PAST VOTE INSTEAD OF VOTE INTENTION
# ══════════════════════════════════════════════════════════════════════════════
#
# Replaces the forward-looking vote intention outcome with past_vote_rrw
# (recalled vote at the most recent election) to verify that the result is not
# driven by how the intention question is worded.  Uses the full sample
# (no year cap needed because past vote does not use migration controls).

bes <- read_parquet('../data/bes_analysis.parquet')

robust_past_vote <- feols(past_vote_rrw ~ sunab(gvar, year) |
                            id + year + msoa11,
                          bes, nthreads = 12, cluster = ~id + year)

save_coeftable(robust_past_vote, 'past_vote_rrw',
               paste0(OUTPUT_DIR, 'bes_robust_past_vote.csv'))

cat("Robustness (past vote) done.\n")


# ══════════════════════════════════════════════════════════════════════════════
# 3. ROBUSTNESS: NOT-YET-TREATED CONTROL GROUP
# ══════════════════════════════════════════════════════════════════════════════
#
# Restricts the control group to not-yet-treated units (gvar != 10000),
# excluding never-treated MSOAs entirely.  This addresses the concern that
# never-treated areas are structurally different from treated ones.

bes <- read_parquet('../data/bes_analysis.parquet')

bes_nyt <- bes[bes$gvar != 10000, ]

robust_nyt <- feols(rrw_vote ~ sunab(gvar, year) |
                      id + year + msoa11,
                    bes_nyt, nthreads = 12, cluster = ~id + year)

save_coeftable(robust_nyt, 'rrw_vote_not_yet_treated',
               paste0(OUTPUT_DIR, 'bes_robust_not_yet_treated.csv'))

# Table A34: coefficient table for not-yet-treated model
nyt_notes <- "Two-way clustered (respondent $\\times$ year) standard errors in parentheses.
  Model includes respondent, year, and MSOA fixed effects, with no additional
  controls. The control group is restricted to not-yet-treated MSOAs
  (\\texttt{gvar} $\\neq$ 10000), excluding never-treated areas.
  ATT is the \\citet{sun2021estimating} aggregated average treatment effect
  on the treated."

etable(
  list(robust_nyt),
  agg   = "att",
  dict  = var_dict,
  title = "Robustness: Not-Yet-Treated Control Group (BES)",
  label = "tab:bes_robust_not_yet_treated",
  notes = nyt_notes,
  tex   = TRUE,
  file  = paste0(ARTICLE_DIR, "bes_robust_not_yet_treated.tex")
)

cat("Robustness (not-yet-treated) done.\n")


# ══════════════════════════════════════════════════════════════════════════════
# 4. ROBUSTNESS: MAINSTREAM PARTY VOTE INTENTIONS
# ══════════════════════════════════════════════════════════════════════════════
#
# Tests whether the effect on right-wing vote intention reflects a broader
# anti-establishment shift or is specific to right-wing parties by running
# the same specification for Conservative, Labour, Lib Dem, and Green vote
# intentions.  Two sets of models: without and with the full control set.
# Sample is capped at < 2023 (migration controls unavailable from 2023).

bes <- read_parquet('../data/bes_analysis.parquet')
bes_parties <- bes[bes$year < 2023, ]

main_parties <- c('conservative_vote', 'labour_vote', 'libdem_vote', 'green_vote')
party_labels <- c('Conservative', 'Labour', 'Lib Dem', 'Green')

# Without controls
party_models_nocontrols <- lapply(main_parties, function(party) {
  feols(as.formula(paste(party, "~ sunab(gvar, year) | id + year + msoa11")),
        bes_parties, nthreads = 12, cluster = ~id + year)
})
names(party_models_nocontrols) <- party_labels

for (i in seq_along(main_parties)) {
  save_coeftable(party_models_nocontrols[[i]], main_parties[i],
                 paste0(OUTPUT_DIR, 'bes_robust_', main_parties[i], '.csv'))
}

# With controls
party_models_controls <- lapply(main_parties, function(party) {
  feols(as.formula(paste(party,
    "~ sunab(gvar, year) + IMD_Score + employment_rate +",
    "international_migration_per_pop + migrant_gp_registrations_per_pop |",
    "id + year + msoa11")),
    bes_parties, nthreads = 12, cluster = ~id + year)
})
names(party_models_controls) <- party_labels

for (i in seq_along(main_parties)) {
  save_coeftable(party_models_controls[[i]], main_parties[i],
                 paste0(OUTPUT_DIR, 'bes_robust_controls_', main_parties[i], '.csv'))
}

# Tables
party_notes_nocontrols <- "Two-way clustered (respondent $\\times$ year) standard errors in parentheses.
  All models include respondent, year, and MSOA fixed effects, with no additional
  controls. ATT is the \\citet{sun2021estimating} aggregated average treatment
  effect on the treated. Sample restricted to 2013--2022."

party_notes_controls <- "Two-way clustered (respondent $\\times$ year) standard errors in parentheses.
  All models include respondent, year, and MSOA fixed effects. Controls: IMD Score,
  Employment Rate, International Migration, and Migrant GP Registrations.
  ATT is the \\citet{sun2021estimating} aggregated average treatment effect on the
  treated. Sample restricted to 2013--2022."

etable(
  party_models_nocontrols,
  agg   = "att",
  dict  = var_dict,
  title = "Effects of GP Practice Closures on Mainstream Party Vote Intentions (BES, No Controls)",
  label = "tab:bes_robust_mainstream_parties",
  notes = party_notes_nocontrols,
  tex   = TRUE,
  file  = paste0(ARTICLE_DIR, "bes_robust_mainstream_parties.tex")
)

etable(
  party_models_controls,
  agg   = "att",
  dict  = var_dict,
  title = "Effects of GP Practice Closures on Mainstream Party Vote Intentions (BES, With Controls)",
  label = "tab:bes_robust_mainstream_parties_controls",
  notes = party_notes_controls,
  tex   = TRUE,
  file  = paste0(ARTICLE_DIR, "bes_robust_mainstream_parties_controls.tex")
)

cat("Robustness (mainstream parties) done.\n")


# ══════════════════════════════════════════════════════════════════════════════
# 5. ROBUSTNESS: OPEN-AFTER-CLOSE TREATMENT ASSIGNMENT (Section 6.3)
# ══════════════════════════════════════════════════════════════════════════════
#
# Some MSOAs see a new practice open shortly after a closure.  These cases are
# ambiguous: residents may experience brief disruption but then regain access.
# The open_after_close indicator flags such MSOAs.  We test two alternative
# treatment assignments:
#
#   (a) As treated: open_after_close MSOAs that would otherwise be treated
#       have their treatment indicator set to 0 (i.e. they are re-coded as
#       treated but only from the first non-open_after_close closure).
#       [Original script comment: "if open_after_close AND treatment == 1,
#        set treatment = 0" then re-derive gvar from the remaining treated obs.]
#
#   (b) As controls: open_after_close MSOAs are recoded as never-treated
#       (treatment = 0 regardless of other closures), effectively treating
#       quick-reopening cases as non-events.
#
# Both variants re-derive gvar from the modified treatment column.

# (a) Open-after-close MSOAs: recode only the flagged treatment = 1 rows as 0,
#     then re-derive the cohort variable (first treatment year per MSOA).
bes <- read_parquet('../data/bes_analysis.parquet')

bes <- bes %>%
  mutate(treatment = ifelse(open_after_close == 1 & treatment == 1, 0, treatment))

first_treat_a <- bes %>%
  filter(treatment == 1) %>%
  group_by(msoa11) %>%
  summarise(first_treat_year = min(year)) %>%
  ungroup()

bes <- bes %>%
  left_join(first_treat_a, by = 'msoa11') %>%
  mutate(first_treat_year = ifelse(is.na(first_treat_year), 10000, first_treat_year))

robust_oac_treated <- feols(rrw_vote ~ sunab(first_treat_year, year) |
                              id + year + msoa11,
                            bes, nthreads = 12, cluster = ~id + year)

save_coeftable(robust_oac_treated, 'rrw_vote',
               paste0(OUTPUT_DIR, 'bes_robust_open_after_close_as_treated.csv'))


# (b) Open-after-close MSOAs: recode ALL treatment rows as 0, then re-derive
#     the cohort variable.
bes <- read_parquet('../data/bes_analysis.parquet')

bes <- bes %>%
  mutate(treatment = ifelse(open_after_close == 1, 0, treatment))

first_treat_b <- bes %>%
  filter(treatment == 1) %>%
  group_by(msoa11) %>%
  summarise(first_treat_year = min(year)) %>%
  ungroup()

bes <- bes %>%
  left_join(first_treat_b, by = 'msoa11') %>%
  mutate(first_treat_year = ifelse(is.na(first_treat_year), 10000, first_treat_year))

robust_oac_controls <- feols(rrw_vote ~ sunab(first_treat_year, year) |
                               id + year + msoa11,
                             bes, nthreads = 12, cluster = ~id + year)

save_coeftable(robust_oac_controls, 'rrw_vote',
               paste0(OUTPUT_DIR, 'bes_robust_open_after_close_as_controls.csv'))


# Joint table
oac_notes <- "Two-way clustered (respondent $\\times$ year) standard errors in parentheses.
  Both models include respondent, year, and MSOA fixed effects, no additional
  controls.  \\textit{As treated}: open-after-close MSOAs that had treatment = 1
  are recoded to treatment = 0 and re-derive their first-treatment year from
  any remaining closures.  \\textit{As controls}: all open-after-close MSOAs
  are set to treatment = 0 (never-treated). ATT is the
  \\citet{sun2021estimating} aggregated average treatment effect on the treated."

etable(
  list("As treated" = robust_oac_treated, "As controls" = robust_oac_controls),
  agg   = "att",
  dict  = var_dict,
  title = "Robustness: Open-After-Close Treatment Assignment (BES)",
  label = "tab:bes_robust_open_after_close",
  notes = oac_notes,
  tex   = TRUE,
  file  = paste0(ARTICLE_DIR, "bes_robust_open_after_close.tex")
)

cat("Robustness (open-after-close) done.\n")


# ══════════════════════════════════════════════════════════════════════════════
# 6. MATRIX COMPLETION ATT  [slow — skipped when TEST_RUN = TRUE]
# ══════════════════════════════════════════════════════════════════════════════
#
# An additional robustness check for the primary BES analysis using the matrix
# completion estimator (Athey et al. 2021) implemented in the fect package.
# Unlike Sun & Abraham (2021), this approach imputes counterfactual outcomes
# via low-rank matrix factorisation, and is not a two-way FE model.
#
# Key settings:
#   method = "mc"     – matrix completion (not interactive FE)
#   r = c(0, 5)       – cross-validate over 0–5 latent factors
#   k = 5             – 5-fold CV
#   min.T0 = 3        – require at least 3 pre-treatment periods
#   nboots = 1000     – bootstrap SEs
#   fill.missing = TRUE – impute missing cells before factorisation
#
# The index is constructed as msoa_id = msoa11 + "_" + id to create a unique
# unit identifier at the respondent–MSOA level, since fect requires a
# balanced (or fill.missing = TRUE) panel indexed by a single unit column.
#
# Output: ATT estimates per period saved to
#   ../output_data_for_figures/bes_matrix_completion_att.csv

if (!TEST_RUN) {

  bes <- read_parquet('../data/bes_analysis.parquet')
  bes$msoa_id <- paste0(bes$msoa11, "_", bes$id)

  mc1 <- fect(
    rrw_vote ~ treatment,
    data         = bes,
    method       = "mc",
    index        = c("msoa_id", "wave"),
    r            = c(0, 5),
    k            = 5,
    min.T0       = 3,
    na.rm        = FALSE,
    CV           = TRUE,
    cv.treat     = TRUE,
    force        = "two-way",
    fill.missing = TRUE,
    se           = TRUE,
    parallel     = TRUE,
    nboots       = 1000
  )

  write.csv(data.frame(mc1$est.att),
            paste0(OUTPUT_DIR, 'bes_matrix_completion_att.csv'))

  cat("Matrix completion done.\n")

} else {
  cat("TEST_RUN = TRUE: matrix completion skipped.\n")
}


cat("\nAll done.\n")
