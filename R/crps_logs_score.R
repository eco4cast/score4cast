
## Requires that forecasts and targets have already been cleaned & pivoted!

#' crps_logs_score
#' 
#' Compute the CRPS and LOGS score given a forecast in either ensemble or 
#' normal distribution. (Support for additional distributions to come.)
#' @param forecast a forecast data.frame in long EFI-standard format
#' @param target a target data.frame in long EFI-standard format
#' @export
crps_logs_score <- function(forecast, target) {

  suppressMessages({ # don't  tell me what we are joining by.
  joined <- 
    dplyr::inner_join(forecast, target) |> 
    map_old_format() # helper routine for backwards compatibility, eventually should be deprecated!
  })
  
  # groups are required, no group_by(any_of())
  scores <- joined |> 
  dplyr::group_by(model_id, start_time, site_id, time, family, variable) |> 
  dplyr::summarise(
    observed = observed,
    crps = generic_crps(family, parameter, predicted, observed),
    logs = generic_logs(family, parameter, predicted, observed),
    mean = generic_mean(family, parameter, predicted),
    sd = generic_sd(family, parameter, predicted),
    quantile02.5 = generic_quantile(0.025, family, parameter, predicted, observed),
    quantile10 = generic_quantile(0.10, family, parameter, predicted, observed),
    quantile90 = generic_quantile(0.90, family, parameter, predicted, observed),
    quantile97.5 = generic_quantile(0.975, family, parameter, predicted, observed),
    .groups = "drop"
  )
  
  scores
}

# Should we infer start_time if missing?  


generic_mean <- function(family, parameter, predicted) {
  names(predicted) = parameter
  switch(unique(family),
         norm = predicted["mean"],
         sample = mean(predicted)
  )
}

generic_sd <- function(family, parameter, predicted) {
  names(predicted) = parameter
  switch(unique(family),
         norm = predicted["sd"],
         sample = sd(predicted)
  )
}

generic_crps <- function(family, parameter, predicted, observed){
  # scoringRules already has a generic crps() method that covers 
  # all cases except crps_sample.  (which seems to be an oversight)
  
  names(predicted) = parameter
  args <- c(list(y = observed[[1]], family = family), as.list(predicted))
  switch(unique(family),
         sample = crps_sample(observed[[1]], predicted),
         do.call(crps, args)
  )
}

generic_logs <- function(family, parameter, predicted, observed){
  # scoringRules already has a generic crps() method that covers 
  # all cases except crps_sample.  (which seems to be an oversight)
  
  names(predicted) = parameter
  args <- c(list(y = observed[[1]], family = family), as.list(predicted))
  switch(unique(family),
         sample = logs_sample(observed[[1]], predicted),
         do.call(logs, args)
  )
}

generic_quantile <- function(p, family, parameter, predicted, observed){
  names(predicted) = parameter
  switch(unique(family),
         norm =  stats::qnorm(p, mean =  predicted["mean"], sd = predicted["sd"]),
         sample = stats::quantile(predicted, p, na.rm = TRUE)
  )
}



## Teach crps to treat any NA observations as NA scores:
crps_sample <- function(y, dat) {
  tryCatch(scoringRules::crps_sample(y, dat),
           error = function(e) NA_real_, finally = NA_real_)
}

crps <- function(y, ...) {
  tryCatch(scoringRules::crps(y, ...),
           error = function(e) NA_real_, finally = NA_real_)
}

## Teach crps to treat any NA observations as NA scores:
logs_sample <- function(y, dat) {
  tryCatch(scoringRules::logs_sample(y, dat),
           error = function(e) NA_real_, finally = NA_real_)
}

logs <- function(y, ...) {
  tryCatch(scoringRules::logs(y, ...),
           error = function(e) NA_real_, finally = NA_real_)
}

logs_norm <- function(y, mean, sd) {
  tryCatch(scoringRules::logs_norm(y, mean = mean, sd = sd),
           error = function(e) NA_real_, finally = NA_real_)
}



globalVariables(c("crps" ,"filename",
                  "family", "site_id", "parameter", "ensemble",
                  "horizon", "model_id", "target_id",
                  "logs","observed", "pub_time", "start_time",
                  "predicted", "variable", "interval",
                  "statistic", "time",
                  "mean", "sd", "forecast",
                  "siteID", "plotID", "height", "depth"), package="score4cast")

