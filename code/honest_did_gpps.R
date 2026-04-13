
# HonestDiD Sensitivity Analysis — GP Patient Survey (GPPS)
# Implements the Rambachan & Roth (2023) sensitivity analysis for violations of
# parallel trends. Run from the code/ directory (same working directory convention
# as statistical_analysis.R).
#
# Outputs (→ ../final_output_for_article/):
#   honestdid_rm_gpps_positive_t0_t5.png  — Figure A4: per-period RM sensitivity, positive outcome
#   honestdid_rm_gpps_negative_t0_t5.png  — Figure A5: per-period RM sensitivity, negative outcome

library(HonestDiD)   # remotes::install_github("asheshrambachan/HonestDiD")
library(fixest)
library(nanoparquet)
library(ggplot2)
library(patchwork)
library(dplyr)

OUTPUT_DIR <- '../final_output_for_article/'
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)


# ── Data ─────────────────────────────────────────────────────────────────────

GPPS_data <- nanoparquet::read_parquet('../data/gp_patient_survey_panel.parquet')



# ── Sun & Abraham models (identical specification to statistical_analysis.R) ─

gpps_positive <- feols(
  positive_overall_experience_with_gp_practice ~ sunab(gvar, year) |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')

gpps_negative <- feols(
  negative_overall_experience_making_an_appointment ~ sunab(gvar, year) |
  oslaua + year,
  GPPS_data, nthreads = 10, vcov = 'twoway')




# ── Helper: extract aggregated event-time beta and VCV from a sunab model ────
#
#' @description
#' Takes a regression estimated using fixest with the sunab option and extracts
#' the aggregated event-study coefficients and their variance-covariance matrix.
#' @param sunab_fixest The result of a fixest call using the sunab option.
#' @returns A list: beta (event-study coefficients), sigma (VCV matrix),
#'          cohorts (relative times corresponding to beta/sigma).

sunab_beta_vcv <- function(sunab_fixest) {

  sunab_agg   <- sunab_fixest$model_matrix_info$sunab$agg_period
  sunab_names <- base::names(sunab_fixest$coefficients)
  sunab_sel   <- base::grepl(sunab_agg, sunab_names, perl = TRUE)
  sunab_names <- sunab_names[sunab_sel]

  if (!base::is.null(sunab_fixest$weights)) {
    sunab_wgt <- base::colSums(sunab_fixest$weights *
      base::sign(stats::model.matrix(sunab_fixest)[, sunab_names, drop = FALSE]))
  } else {
    sunab_wgt <- base::colSums(
      base::sign(stats::model.matrix(sunab_fixest)[, sunab_names, drop = FALSE]))
  }

  sunab_cohorts <- base::as.numeric(base::gsub(
    base::paste0(".*", sunab_agg, ".*"), "\\2", sunab_names, perl = TRUE))
  sunab_mat   <- stats::model.matrix(~ 0 + base::factor(sunab_cohorts))
  sunab_trans <- base::solve(base::t(sunab_mat) %*% (sunab_wgt * sunab_mat)) %*%
                 base::t(sunab_wgt * sunab_mat)

  sunab_coefs <- sunab_trans %*% base::cbind(sunab_fixest$coefficients[sunab_sel])
  sunab_vcov  <- sunab_trans %*%
                 sunab_fixest$cov.scaled[sunab_sel, sunab_sel] %*%
                 base::t(sunab_trans)

  base::return(base::list(
    beta    = sunab_coefs,
    sigma   = sunab_vcov,
    cohorts = base::sort(base::unique(sunab_cohorts))
  ))
}


# ── Sensitivity analysis: relative magnitudes ─────────────────────────────────
#
# run_sensitivity() runs the Rambachan & Roth (2023) relative-magnitude
# sensitivity analysis and saves the plot to article/figures/.
# l_vec is left at its default (first post-treatment period, l=0).

run_sensitivity <- function(model, article_label) {

  beta_vcv <- sunab_beta_vcv(model)

  kwargs <- list(
    betahat        = beta_vcv$beta,
    sigma          = beta_vcv$sigma,
    numPrePeriods  = sum(beta_vcv$cohorts < 0),
    numPostPeriods = sum(beta_vcv$cohorts > -1)
  )
  extra <- list(Mbarvec = seq(from = 0.5, to = 2, by = 0.5), gridPoints = 100)

  original_results    <- do.call(HonestDiD::constructOriginalCS, kwargs)
  sensitivity_results <- do.call(
    HonestDiD::createSensitivityResults_relativeMagnitudes,
    c(kwargs, extra)
  )

  p <- HonestDiD::createSensitivityPlot_relativeMagnitudes(
    sensitivity_results, original_results
  )

  out_path <- paste0(OUTPUT_DIR, "honestdid_rm_", article_label, ".png")
  ggsave(out_path, p, width = 8, height = 5, dpi = 300)
  cat(sprintf("Saved: %s\n", out_path))

  # Report key numbers
  cat(sprintf("  Original 95%% CI:        [%+.4f, %+.4f]\n",
              original_results$lb, original_results$ub))
  mbar_rows <- which(!is.na(sensitivity_results$lb))
  if (length(mbar_rows) > 0) {
    excl <- (sensitivity_results$lb[mbar_rows] > 0) |
            (sensitivity_results$ub[mbar_rows] < 0)
    if (all(excl)) {
      cat("  Robust CI excludes zero for all Mbar values tested.\n")
    } else {
      first_incl <- mbar_rows[which(!excl)[1]]
      cat(sprintf("  Robust CI first includes zero at Mbar = %.2f\n",
                  sensitivity_results$Mbar[first_incl]))
    }
  }

  invisible(list(original = original_results, sensitivity = sensitivity_results))
}


# ── Multi-period sensitivity: t=0 through t=5 ────────────────────────────────
#
# run_sensitivity_t0_t5() runs the RM sensitivity analysis separately for each
# post-treatment period from t=0 to t=5, producing one panel per period.
# The six panels are assembled into a 2×3 subplot with patchwork and saved.

run_sensitivity_t0_t5 <- function(model, article_label,
                                  periods   = 0:5,
                                  Mbarvec   = seq(0.5, 2, by = 0.5),
                                  gridPoints = 100) {

  beta_vcv   <- sunab_beta_vcv(model)
  post_times <- sort(beta_vcv$cohorts[beta_vcv$cohorts >= 0])
  periods    <- periods[periods %in% post_times]   # keep only periods that exist

  numPre  <- sum(beta_vcv$cohorts < 0)
  numPost <- length(post_times)

  plots <- vector("list", length(periods))

  for (i in seq_along(periods)) {
    t   <- periods[i]
    idx <- which(post_times == t)

    l_vec        <- rep(0, numPost)
    l_vec[idx]   <- 1

    kwargs <- list(
      betahat        = beta_vcv$beta,
      sigma          = beta_vcv$sigma,
      numPrePeriods  = numPre,
      numPostPeriods = numPost,
      l_vec          = l_vec
    )

    orig <- do.call(HonestDiD::constructOriginalCS, kwargs)
    sens <- do.call(
      HonestDiD::createSensitivityResults_relativeMagnitudes,
      c(kwargs, list(Mbarvec = Mbarvec, gridPoints = gridPoints))
    )

    p <- HonestDiD::createSensitivityPlot_relativeMagnitudes(sens, orig) +
      labs(title = bquote(italic(l) == .(t))) +
      theme_minimal(base_size = 9) +
      theme(
        plot.title   = element_text(hjust = 0.5, size = 10),
        axis.title   = element_text(size = 8),
        axis.title.y = if (i %% 3 != 1) element_blank() else element_text(size = 8),
        legend.position = "none"
      )

    plots[[i]] <- p

    # console summary
    excl <- !is.na(sens$lb) & ((sens$lb > 0) | (sens$ub < 0))
    if (all(excl)) {
      breakdown <- "> 2.00 (all tested)"
    } else {
      breakdown <- sprintf("%.2f", sens$Mbar[which(!excl)[1]])
    }
    cat(sprintf("  t=%d: OCI [%+.4f, %+.4f]  breakdown Mbar = %s\n",
                t, orig$lb, orig$ub, breakdown))
  }

  combined <- patchwork::wrap_plots(plots, nrow = 2, ncol = 3) +
    patchwork::plot_annotation(
      caption = expression(bar(M) ~ "= max post-treatment violation as multiple of largest pre-trend deviation"),
      theme   = theme(plot.caption = element_text(hjust = 0.5, size = 8))
    )

  out_path <- paste0(OUTPUT_DIR, "honestdid_rm_", article_label, "_t0_t5.png")
  ggsave(out_path, combined, width = 12, height = 7, dpi = 300)
  cat(sprintf("Saved: %s\n", out_path))

  invisible(plots)
}


# ── Run for both GPPS outcomes ────────────────────────────────────────────────

cat("\n══ Multi-period sensitivity: Positive overall experience ══\n")
run_sensitivity_t0_t5(gpps_positive, "gpps_positive")

cat("\n══ Multi-period sensitivity: Negative appointment experience ══\n")
run_sensitivity_t0_t5(gpps_negative, "gpps_negative")
