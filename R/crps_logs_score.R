
## Requires that forecasts and targets have already been cleaned & pivoted!

#' crps_logs_score
#' 
#' Compute the CRPS and LOGS score given a forecast in either ensemble or 
#' normal distribution. (Support for additional distributions to come.)
#' @param forecast a forecast data.frame in long EFI-standard format
#' @param target a target data.frame in long EFI-standard format
#' @export
crps_logs_score <- function(forecast, target) {
  
  target <- target |>
    select(time, site_id, variable, observed)
  
  suppressMessages({ # don't  tell me what we are joining by.
  joined <- 
    dplyr::left_join(forecast, target, by = c("site_id", "time", "variable")) |> 
    map_old_format() # helper routine for backwards compatibility, eventually should be deprecated!
  })
  
  # groups are required, no group_by(any_of())
  scores <- joined |> 
  dplyr::group_by(model_id, start_time, site_id, time, family, variable) |> 
  dplyr::summarise(
     observed = unique(observed), # grouping vars define a unique obs
    crps = generic_crps(family, parameter, predicted, observed),
    logs = generic_logs(family, parameter, predicted, observed),
    mean = generic_mean(family, parameter, predicted),
    sd = generic_sd(family, parameter, predicted),
    quantile02.5 = generic_quantile(0.025, family, parameter, predicted),
    quantile10 = generic_quantile(0.10, family, parameter, predicted),
    quantile90 = generic_quantile(0.90, family, parameter, predicted),
    quantile97.5 = generic_quantile(0.975, family, parameter, predicted),
    .groups = "drop"
  )
  
  scores
}

## Naming conventions are based on `distributional` package:
## https://pkg.mitchelloharawild.com/distributional/reference/index.html

## More generically, map family, parameter -> list-column of type .distribution,
## then we can use distributional:: functions
infer_dist <- function(family, parameter, predicted) {
  names(predicted) = parameter
  
  ## operates on a unique observation (model_id, start_time, site_id, time, family, variable)
  fam <- unique(family)
  arg <- switch(fam, 
                sample = list(list(predicted)),
                as.list(predicted)
  )
  fn <- eval(rlang::parse_expr(paste0("distributional::dist_", fam)))
  dist <- do.call(fn, arg)
  dist
}


generic_mean <- function(family, parameter, predicted) {
  dist <- infer_dist(family, parameter, predicted)
  mean(dist)
}

generic_sd <- function(family, parameter, predicted) {
  dist <- infer_dist(family, parameter, predicted)
  sqrt(distributional::variance(dist))
}


generic_quantile <- function(p, family, parameter, predicted){
  # NOTE:
  # hilo(dist_normal(0,1),95)
  # is essentially the same as:
  # qnorm(.975, 0,1); qnorm(.025, 0,1)
  
  ## Uglify equivalent:
  x <- abs(100 - 2* ( 1 - p) * 100) 
  dist <- infer_dist(family, parameter, predicted)
  interval <- distributional::hilo(dist, x)
  if(p > 0.5) 
    interval$upper
  else
    interval$lower
}

generic_crps <- function(family, parameter, predicted, observed){
  names(predicted) = parameter
  y <- dplyr::first(observed)
  tryCatch(
  switch(unique(family),
         lognormal = scoringRules::crps_lnorm(y, predicted['mu'], predicted['sigma']),
         normal = scoringRules::crps_norm(y, predicted['mu'], predicted['sigma']),
         sample = scoringRules::crps_sample(y, predicted)
  ),
  error = function(e) NA_real_, finally = NA_real_)
}


generic_logs <- function(family, parameter, predicted, observed){
  names(predicted) = parameter
  y <- dplyr::first(observed)
  tryCatch(
    switch(unique(family),
           lognormal = scoringRules::logs_lnorm(y, predicted['mu'], predicted['sigma']),
           normal = scoringRules::logs_norm(y, predicted['mu'], predicted['sigma']),
           sample = scoringRules::logs_sample(y, predicted)
    ),
    error = function(e) NA_real_, finally = NA_real_)
}

# scoringRules already has a generic crps() method that covers all cases except crps_sample.
# if we used those conventions, we could get all parameters for free as:
# args <- c(list(y = dplyr::first(observed), family = family), as.list(predicted))
# do.call(logs, args)





globalVariables(c("crps" ,"filename",
                  "family", "site_id", "parameter", "ensemble",
                  "horizon", "model_id", "target_id",
                  "logs","observed", "pub_time", "start_time",
                  "predicted", "variable", "interval",
                  "statistic", "time",
                  "mean", "sd", "forecast",
                  "siteID", "plotID", "height", "depth"), package="score4cast")

