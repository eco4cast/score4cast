
## Requires that forecasts and targets have already been cleaned & pivoted!

#' crps_logs_score
#' 
#' Compute the CRPS and LOGS score given a forecast in either ensemble or 
#' normal distribution. (Support for additional distributions to come.)
#' @param forecast a forecast data.frame in long EFI-standard format
#' @param target a target data.frame in long EFI-standard format
#' @param extra_groups character vector of additional groups to use for scoring
#' @export
crps_logs_score <- function(forecast, target, extra_groups = NULL, include_summaries = TRUE) {
  
  target <- target |>
    standardize_target() |> 
    dplyr::select(dplyr::any_of(c("datetime", "site_id", "variable", "observation", extra_groups)))
  
  # no longer run full 'standardize' call here.
  joined <- forecast |> recode("family", from="ensemble", to="sample") |>
    dplyr::left_join(target, by = c("site_id", "datetime", "variable", extra_groups))
  
  # use with across(any_of()) to avoid bare names; allows optional terms
  grouping <- c("model_id", "reference_datetime", "site_id", 
                "datetime", "family", "variable", "pubDate", "pub_datetime", extra_groups)
  
  if(include_summaries){
    
    scores <- joined |> 
      dplyr::group_by(dplyr::across(dplyr::any_of(grouping))) |> 
      dplyr::summarise(
        observation = unique(observation), # grouping vars define a unique obs
        crps = generic_crps(family, parameter, prediction, observation),
        logs = generic_logs(family, parameter, prediction, observation),
        dist = infer_dist(family, parameter, prediction),
        .groups = "drop") |> 
      dplyr::mutate(
        mean = as.numeric(mean(dist)),
        median = as.numeric(stats::median(dist)),
        sd = sqrt(as.numeric(distributional::variance(dist))),
        quantile97.5 = as.numeric(distributional::hilo(dist, 95)$upper),
        quantile02.5 = as.numeric(distributional::hilo(dist, 95)$lower),
        quantile90 = as.numeric(distributional::hilo(dist, 90)$upper),
        quantile10 = as.numeric(distributional::hilo(dist, 90)$lower)) |> 
      dplyr::select(-dist)
    
  }else{
    scores <- joined |> 
      dplyr::group_by(dplyr::across(dplyr::any_of(grouping))) |> 
      dplyr::summarise(
        observation = unique(observation), # grouping vars define a unique obs
        crps = generic_crps(family, parameter, prediction, observation),
        logs = generic_logs(family, parameter, prediction, observation),
        .groups = "drop")
  }
  scores
}

#' summarize_forecast
#' 
#' Compute summaries of a forecast for ensemble and parametric forecasts
#' @param forecast a forecast data.frame in long EFI-standard format
#' @param extra_groups character vector of additional groups to use for grouping summaries
#' @export
summarize_forecast <- function(forecast, extra_groups = NULL) {
  
  # use with across(any_of()) to avoid bare names; allows optional terms
  grouping <- c("model_id", "reference_datetime", "site_id", 
                "datetime", "family", "variable", "pubDate","pub_datetime", extra_groups)
  
  forecast |> 
    dplyr::mutate(family = ifelse(family == "ensemble", "sample", family)) |>
    dplyr::group_by(dplyr::across(dplyr::any_of(grouping))) |>
    dplyr::summarise(dist = score4cast:::infer_dist(family, parameter, prediction)) |>
    dplyr::mutate(
      mean = as.numeric(mean(dist)),
      median = as.numeric(stats::median(dist)),
      sd = sqrt(as.numeric(distributional::variance(dist))),
      quantile97.5 = as.numeric(distributional::hilo(dist, 95)$upper),
      quantile02.5 = as.numeric(distributional::hilo(dist, 95)$lower),
      quantile90 = as.numeric(distributional::hilo(dist, 90)$upper),
      quantile10 = as.numeric(distributional::hilo(dist, 90)$lower)
          ) |> 
    dplyr::select(-dist) |>
    dplyr::ungroup()
}



## Naming conventions are based on `distributional` package:
## https://pkg.mitchelloharawild.com/distributional/reference/index.html



generic_crps <- function(family, parameter, prediction, observation){
  names(prediction) = parameter
  y <- dplyr::first(observation)
  tryCatch(
    switch(unique(as.character(family)),
           lognormal = scoringRules::crps_lnorm(y, prediction['mu'], prediction['sigma']),
           normal = scoringRules::crps_norm(y, prediction['mu'], prediction['sigma']),
           bernoulli = scoringRules::crps_binom(y, size = 1, prediction['prob']),
           beta = scoringRules::crps_beta(y, shape1 = prediction['shape1'], shape1 = prediction['shape2']),
           uniform = scoringRules::crps_unif(y, min = prediction['min'], max = prediction['max']),
           gamma = scoringRules::crps_gamma(y, shape = prediction['shape'], rate = prediction['rate']),
           logistic = scoringRules::crps_logis(y, location = prediction['location'], scale = prediction['scale']),
           exponential = scoringRules::crps_exp(y, rate = prediction['rate']),
           poisson = scoringRules::crps_pois(y, lambda = prediction['lambda']),
           sample = scoringRules::crps_sample(y, prediction)
    ),
    error = function(e) NA_real_, finally = NA_real_)
}


generic_logs <- function(family, parameter, prediction, observation){
  names(prediction) = parameter
  y <- dplyr::first(observation)
  tryCatch(
    switch(unique(as.character(family)),
           lognormal = scoringRules::logs_lnorm(y, prediction['mu'], prediction['sigma']),
           normal = scoringRules::logs_norm(y, prediction['mu'], prediction['sigma']),
           bernoulli = scoringRules::logs_binom(y, size = 1, prediction['prob']),
           beta = scoringRules::logs_beta(y, shape1 = prediction['shape1'], shape1 = prediction['shape2']),
           uniform = scoringRules::logs_unif(y, min = prediction['min'], max = prediction['max']),
           gamma = scoringRules::logs_gamma(y, shape = prediction['shape'], rate = prediction['rate']),
           logistic = scoringRules::logs_logis(y, location = prediction['location'], scale = prediction['scale']),
           exponential = scoringRules::logs_exp(y, rate = prediction['rate']),
           poisson = scoringRules::logs_pois(y, lambda = prediction['lambda']),
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



