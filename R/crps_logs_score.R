
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
    standardize_target() |> 
    dplyr::select("datetime", "site_id", "variable", "observation")
  
  joined <- forecast |> 
    dplyr::left_join(target, by = c("site_id", "datetime", "variable"))
  
  # use with across(any_of()) to avoid bare names; allows optional terms
  grouping <- c("model_id", "reference_datetime", "site_id", 
                "datetime", "family", "variable")
  
  scores <- joined |> 
      dplyr::group_by(dplyr::across(dplyr::any_of(grouping))) |> 
      dplyr::summarise(
        observation = unique(observation), # grouping vars define a unique obs
        crps = generic_crps(family, parameter, prediction, observation),
        logs = generic_logs(family, parameter, prediction, observation),
        dist = infer_dist(family, parameter, prediction),
        .groups = "drop") |>
      dplyr::mutate(
        mean = mean(dist),
        median = stats::median(dist),
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



generic_crps <- function(family, parameter, prediction, observation){
  names(prediction) = parameter
  y <- dplyr::first(observation)
  tryCatch(
  switch(unique(family),
         lognormal = scoringRules::crps_lnorm(y, prediction['mu'], prediction['sigma']),
         normal = scoringRules::crps_norm(y, prediction['mu'], prediction['sigma']),
         sample = scoringRules::crps_sample(y, prediction)
  ),
  error = function(e) NA_real_, finally = NA_real_)
}


generic_logs <- function(family, parameter, prediction, observation){
  names(prediction) = parameter
  y <- dplyr::first(observation)
  tryCatch(
    switch(unique(family),
           lognormal = scoringRules::logs_lnorm(y, prediction['mu'], prediction['sigma']),
           normal = scoringRules::logs_norm(y, prediction['mu'], prediction['sigma']),
           sample = scoringRules::logs_sample(y, prediction)
    ),
    error = function(e) NA_real_, finally = NA_real_)
}

# scoringRules already has a generic crps() method that covers all cases except crps_sample.
# if we used those conventions, we could get all parameters for free as:
# args <- c(list(y = dplyr::first(observation), family = family), as.list(prediction))
# do.call(logs, args)



globalVariables(c("family", "parameter", "prediction", "observation", "dist"),
                package="score4cast")



