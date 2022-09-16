
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
    dplyr::select("datetime", "site_id", "variable", "observed")
  
  joined <- forecast |> 
    dplyr::left_join(target, by = c("site_id", "datetime", "variable"))
  
  # use with across(any_of()) to avoid bare names; allows optional terms
  grouping <- c("model_id", "reference_datetime", "site_id", 
                "datetime", "family", "variable")
  
  scores <- joined |> 
      dplyr::group_by(dplyr::across(dplyr::any_of(grouping))) |> 
      dplyr::summarise(
        observed = unique(observed), # grouping vars define a unique obs
        crps = generic_crps(family, parameter, predicted, observed),
        logs = generic_logs(family, parameter, predicted, observed),
        dist = infer_dist(family, parameter, predicted),
        .groups = "drop") |>
      dplyr::mutate(
        mean = mean(dist),
        median = median(dist),
        sd = sqrt(distributional::variance(dist)),
        quantile97.5 = distributional::hilo(dist, 95)$upper,
        quantile02.5 = distributional::hilo(dist, 95)$lower,
        quantile90 = distributional::hilo(dist, 90)$upper,
        quantile10 = distributional::hilo(dist, 90)$lower
      ) |> dplyr::select(-dist)
 
 
  scores
}

## Naming conventions are based on `distributional` package:
## https://pkg.mitchelloharawild.com/distributional/reference/index.html



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



globalVariables(c("family", "parameter", "predicted", "observed", "dist"),
                package="score4cast")



