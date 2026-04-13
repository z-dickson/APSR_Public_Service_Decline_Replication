# bes_moderation_analysis.R
#
# Treatment effect heterogeneity: binning estimator for GP practice closure
# effects on right-wing vote intention (BES), moderated by local contextual
# and attitudinal variables.
#
# Uses the interflex package (Hainmueller, Mummolo, and Xu 2019a) with the
# binning estimator (4 bins, evaluated at the 20th, 40th, 60th, and 80th
# percentiles of each moderator) and MSOA + wave fixed effects.
#
# Note on fixed effects: individual (respondent) FEs cannot be used with the
# binning estimator here because the moderators are MSOA-level variables — all
# individuals in the same MSOA land in the same bin, leaving no within-
# individual variation after demeaning.  MSOA + wave FEs absorb the same
# time-invariant geographic confounders as the primary analysis and are
# consistent with treatment being assigned at the MSOA level.
# A companion linear interaction model is estimated for each moderator to
# produce the quartile significance tests saved as LaTeX tables.
#
# Input:
#   ../data/bes_analysis.parquet
#
# Outputs (PNG figures and LaTeX tables → ../final_output_for_article/):
#
#   Figure 8 panels (one PNG per moderator, assembled 2×2 in LaTeX):
#     {IMD_Score,employment_rate,migrant_gp_registrations_per_pop,
#      international_migration_per_pop}_rrw_vote_moderator.png
#
#   LaTeX tables (linear interaction, quartile significance tests):
#     Tables A11–A30: {moderator}_{party}_moderator.tex

library(interflex)
library(nanoparquet)
library(ggplot2)
library(xtable)


# ── Configuration ─────────────────────────────────────────────────────────────

OUTPUT_DIR <- '../final_output_for_article'
dir.create(OUTPUT_DIR, showWarnings = FALSE, recursive = TRUE)


# ── Data ──────────────────────────────────────────────────────────────────────

bes <- read_parquet('../data/bes_analysis.parquet')

# Create a unique MSOA-respondent identifier for use as a fixed effect
bes$msoa_id <- paste0(bes$msoa11, "_", bes$id)

# log the migration variables 
bes$log_international_migration_per_pop <- log((bes$international_migration_per_pop + .001))
bes$log_migrant_gp_registrations_per_pop <- log((bes$migrant_gp_registrations_per_pop + .001))




# moderation of treatment effect estimation using interflex package:

# create basic function that takes in X and Z variables and returns the interflex model
iflex <- function(df, X, Z, Y, var_name = X, Xdistri = "histogram") {
       m1 <- interflex(
              Y = Y,
              D = "treatment",
              X = X,
              Z = Z,
              data = df,
              estimator = "binning",
              cl = 'msoa_id',
              vcov.type = "robust",
              nbins = 4,
              FE = c('msoa_id', 'year'),
              main = "Marginal Effects",
              parallel = TRUE,
              theme.bw = TRUE,
              cores = 10,
              na.rm = TRUE)
       
       x <- plot(m1, 
       ylab = "Marginal Effect of Practice Closure",
       xlab = paste('Moderator:', var_name), 
       hist.color = c("red", "black"),
       hist.color.alpha = 0.6,
       Xdistr = Xdistri,
       file = file.path(OUTPUT_DIR, paste0(X, '_', Y, '_moderator.png')),
       cex.axis = 1.2, cex.lab = 1.5, cex.main = 1.5, cex.sub = 1.5,
       height = 7, width = 9,
       scale = 1,
       )
       # save the plot
       #ggsave(paste('tables/', Y, '_', X, '_moderator.png', sep = ''), plot = x, height = 7, width = 9)
       return(x)
}



# helper that wraps a tabular in \resizebox
wrap_tabular_with_resizebox <- function(texfile, 
                                        width="\\textwidth", 
                                        height="!", 
                                        force_H=TRUE) {
  # Read .tex file
  lines <- readLines(texfile)

  # Optionally replace table placement with [H]
  if (force_H) {
    lines <- gsub("^\\\\begin\\{table\\}\\[.*\\]", "\\\\begin{table}[H]", lines)
  }

  # Find tabular environments
  begin_idx <- grep("^\\\\begin\\{tabular\\}", lines)
  end_idx   <- grep("^\\\\end\\{tabular\\}", lines)

  if (length(begin_idx) != length(end_idx)) {
    stop("Mismatched tabular environments in file.")
  }

  # Insert wrappers (work backwards so indices stay valid)
  for (i in seq_along(begin_idx)) {
    b <- begin_idx[i]
    e <- end_idx[i]

    lines[b] <- paste0("\\resizebox{", width, "}{", height, "}{%\n", lines[b])
    lines[e] <- paste0(lines[e], "\n}% end resizebox")
  }

  # Write back to same file
  writeLines(lines, texfile)
}


# create a basic function that takes in X and Z variables and returns a latex table (we need to change the estimator to linear and re-estimate the model to get the correct results)
create_table <- function(df, X, Z, Y, var_name = X, Xdistri = "histogram") {
       m1 <- interflex(
              Y = Y,
              D = "treatment",
              X = X,
              Z = Z,
              data = df,
              estimator = "linear",
              cl = 'msoa_id',
              vcov.type = "cluster",
              nbins = 4,
              FE = c('msoa_id', 'year'),
              main = "Marginal Effects",
              parallel = TRUE,
              theme.bw = TRUE,
              cores = 10,
              na.rm = TRUE)

       # create latex table with results 
       tab <- inter.test(m1,diff.values=c(0.25,0.5,0.75),percentile=TRUE)
       # create a table of the results
       tab <- as.data.frame(tab)
       # name the moderator
       tab$Moderator <- var_name
       # create xtable

       table <- xtable(tab, caption = paste('Moderator:', var_name), label = paste('tab:', X, '_moderator', sep = ''), digits = 3)


       # rename the columns 
       colnames(table) <- c('Estimate', 'Std. Error', 't-value', 'p-value', '2.5% CI', '97.5% CI', 'Moderator')

       file = file.path(OUTPUT_DIR, paste0(Y, '_', X, '_moderator.tex'))

       print(table, file = file)
       # wrap the tabular in a resizebox
       wrap_tabular_with_resizebox(file)
}






# index of multiple deprivation score

iflex(bes, X = "IMD_Score", 
Z = c("employment_rate", 'log_international_migration_per_pop', 'log_migrant_gp_registrations_per_pop'), 
Y = "rrw_vote", 
var_name = "Index of Multiple Deprivation Score", 
) 


# table: 
create_table(bes, X = "IMD_Score", 
Z = c("employment_rate", 'log_international_migration_per_pop', 'log_migrant_gp_registrations_per_pop'), 
Y = "rrw_vote",
var_name = "Index of Multiple Deprivation Score"
)





# migrant gp registrations proportion

iflex(bes, X = "log_migrant_gp_registrations_per_pop", 
Z = c("employment_rate", 'log_international_migration_per_pop', 'IMD_Score'), 
Y = "rrw_vote",
var_name = "(log) Migrant GP Registrations Per Capita"
)





# table:
create_table(bes, X = "log_migrant_gp_registrations_per_pop", 
Z = c("employment_rate", 'log_international_migration_per_pop', 'IMD_Score'), 
Y = "rrw_vote",
var_name = "(log) Migrant GP Registrations Per Capita"
)





# unemployment rate

iflex(bes, X = "employment_rate", 
Z = c('log_international_migration_per_pop', "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "rrw_vote",
var_name = "Employment Rate"
)

create_table(bes, X = "employment_rate", 
Z = c('log_international_migration_per_pop', "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "rrw_vote",
var_name = "Employment Rate"
)




# inflow_longterm_international_migration_proportion

iflex(bes, X = "log_international_migration_per_pop", 
Z = c("employment_rate", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "rrw_vote",
var_name = "(log) International Migration Per Capita"
)

create_table(bes, X = "log_international_migration_per_pop", 
Z = c("employment_rate", 'log_migrant_gp_registrations_per_pop', 'IMD_Score'), 
Y = "rrw_vote",
var_name = "(log) International Migration Per Capita"
)























# Green vote 

create_table(bes, X = "IMD_Score", 
Z = c("employment_rate", 'log_international_migration_per_pop', 'log_migrant_gp_registrations_per_pop'), 
Y = "green_vote",
var_name = "Index of Multiple Deprivation Score"
)

create_table(bes, X = "log_international_migration_per_pop", 
Z = c("employment_rate", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "green_vote",
var_name = "(log) International Migration Per Capita"
)

create_table(bes, X = "log_migrant_gp_registrations_per_pop", 
Z = c("employment_rate", "log_international_migration_per_pop", 'IMD_Score'), 
Y = "green_vote",
var_name = "(log) Migrant GP Registrations Per Capita"
)

create_table(bes, X = "employment_rate", 
Z = c("log_international_migration_per_pop", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "green_vote",
var_name = "(log) Employment Rate"
)






# Lib Dem vote

create_table(bes, X = "IMD_Score", 
Z = c("employment_rate", 'log_international_migration_per_pop', 'log_migrant_gp_registrations_per_pop'), 
Y = "libdem_vote",
var_name = "Index of Multiple Deprivation Score"
)

create_table(bes, X = "log_international_migration_per_pop", 
Z = c("employment_rate", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "libdem_vote",
var_name = "(log) International Migration Per Capita"
)

create_table(bes, X = "log_migrant_gp_registrations_per_pop", 
Z = c("employment_rate", "log_international_migration_per_pop", 'IMD_Score'), 
Y = "libdem_vote",
var_name = "(log) Migrant GP Registrations Per Capita"
)

create_table(bes, X = "employment_rate", 
Z = c("log_international_migration_per_pop", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "libdem_vote",
var_name = "(log) Employment Rate"
)



# Labour vote

create_table(bes, X = "IMD_Score", 
Z = c("employment_rate", 'log_international_migration_per_pop', 'log_migrant_gp_registrations_per_pop'), 
Y = "labour_vote",
var_name = "Index of Multiple Deprivation Score"
)

create_table(bes, X = "log_international_migration_per_pop", 
Z = c("employment_rate", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "labour_vote",
var_name = "(log) International Migration Per Capita"
)

create_table(bes, X = "log_migrant_gp_registrations_per_pop", 
Z = c("employment_rate", "log_international_migration_per_pop", 'IMD_Score'), 
Y = "labour_vote",
var_name = "(log) Migrant GP Registrations Per Capita"
)

create_table(bes, X = "employment_rate", 
Z = c("log_international_migration_per_pop", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "labour_vote",
var_name = "(log) Employment Rate"
)




# Conservative vote

create_table(bes, X = "IMD_Score", 
Z = c("employment_rate", 'log_international_migration_per_pop', 'log_migrant_gp_registrations_per_pop'), 
Y = "conservative_vote",
var_name = "Index of Multiple Deprivation Score"
)

create_table(bes, X = "log_international_migration_per_pop", 
Z = c("employment_rate", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "conservative_vote",
var_name = "(log) International Migration Per Capita"
)

create_table(bes, X = "log_migrant_gp_registrations_per_pop", 
Z = c("employment_rate", "log_international_migration_per_pop", 'IMD_Score'), 
Y = "conservative_vote",
var_name = "(log) Migrant GP Registrations Per Capita"
)


create_table(bes, X = "employment_rate", 
Z = c("log_international_migration_per_pop", "log_migrant_gp_registrations_per_pop", 'IMD_Score'), 
Y = "conservative_vote",
var_name = "(log) Employment Rate"
)








