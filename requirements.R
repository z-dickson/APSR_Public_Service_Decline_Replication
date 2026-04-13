# requirements.R — install and verify R packages required by the replication pipeline
#
# Run directly:  Rscript requirements.R
# Also sourced automatically by code/main.py before running analysis scripts.
#
# CRAN packages tested with R 4.2+:
#   fect 2.0.5, fixest 0.13.2, nanoparquet 0.3.1, dplyr 1.2.0,
#   ggplot2 4.0.2, patchwork 1.3.2, interflex 1.2.6, xtable 1.8.4
#
# GitHub packages:
#   HonestDiD 0.2.6  (asheshrambachan/HonestDiD)

cran_pkgs <- c(
  "fect",
  "fixest",
  "nanoparquet",
  "dplyr",
  "ggplot2",
  "patchwork",
  "interflex",
  "xtable"
)

missing_cran <- cran_pkgs[!sapply(cran_pkgs, requireNamespace, quietly = TRUE)]
if (length(missing_cran) > 0) {
  message("Installing missing CRAN packages: ", paste(missing_cran, collapse = ", "))
  install.packages(missing_cran, repos = "https://cloud.r-project.org")
}

# HonestDiD is only available from GitHub
if (!requireNamespace("HonestDiD", quietly = TRUE)) {
  message("Installing HonestDiD from GitHub...")
  if (!requireNamespace("remotes", quietly = TRUE)) {
    install.packages("remotes", repos = "https://cloud.r-project.org")
  }
  remotes::install_github("asheshrambachan/HonestDiD")
}

# Verify all packages are now available
all_pkgs <- c(cran_pkgs, "HonestDiD")
still_missing <- all_pkgs[!sapply(all_pkgs, requireNamespace, quietly = TRUE)]
if (length(still_missing) > 0) {
  stop("The following R packages could not be installed: ",
       paste(still_missing, collapse = ", "))
}

cat("All R packages verified.\n")
