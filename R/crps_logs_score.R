

## Requires that forecasts and targets have already been cleaned & pivoted!

#' crps_logs_score
#' 
#' Compute the CRPS and LOGS score given a forecast in either ensemble or 
#' normal distribution. (Support for additional distributions to come.)
#' @param forecast a forecast data.frame in long EFI-standard format
#' @param target a target data.frame in long EFI-standard format
#' @export
crps_logs_score <- function(forecast, target){
  
  # left join will keep predictions even where we have no observations
  joined <- dplyr::left_join(forecast, 
                             target, 
                             by = c("target_id", "site_id", "x", "y",
                                    "z", "time", "variable"))
  
  if("ensemble" %in% colnames(joined)){
    out <- joined %>% 
      group_by(across(-any_of(c("ensemble", "predicted")))) %>% 
      summarise(mean = mean(predicted, na.rm =TRUE),
                sd = stats::sd(predicted, na.rm =TRUE),
                crps = crps_sample(observed[[1]], na_rm(predicted)),
                logs = logs_sample(observed[[1]], na_rm(predicted)),
                quantile02.5 = stats::quantile(predicted, 0.025, na.rm = TRUE),
                quantile10 = stats::quantile(predicted, 0.10, na.rm = TRUE),
                quantile90 = stats::quantile(predicted, 0.90, na.rm = TRUE),
                quantile97.5 = stats::quantile(predicted, 0.975, na.rm = TRUE),
                .groups = "drop")
    
  } else {
    out <- joined  %>% 
      dplyr::mutate(crps = crps_norm(observed, mean, sd),
                    logs = logs_norm(observed, mean, sd),
                    quantile02.5 = stats::qnorm( 0.025, mean, sd),
                    quantile10 = stats::qnorm(0.10, mean, sd),
                    quantile90 = stats::qnorm(0.90, mean, sd),
                    quantile97.5 = stats::qnorm(0.975, mean, sd))
    
  }
  
  ## Ensure both ensemble and stat-based have identical column order:
  out %>% select(any_of(c("target_id", "model_id",
                          "pub_time", "site_id",
                          "x", "y", "z", "time",
                          "variable", "mean", "sd", "observed", "crps",
                          "logs", "quantile02.5", "quantile10",
                          "quantile90","quantile97.5","interval", 
                          "start_time")))
}



## Teach crps to treat any NA observations as NA scores:
crps_sample <- function(y, dat) {
  tryCatch(scoringRules::crps_sample(y, dat),
           error = function(e) NA_real_, finally = NA_real_)
}

crps_norm <- function(y, mean, sd) {
  tryCatch(scoringRules::crps_norm(y, mean = mean, sd = sd),
           error = function(e) NA_real_, finally = NA_real_)
}

## Teach crps to treat any NA observations as NA scores:
logs_sample <- function(y, dat) {
  tryCatch(scoringRules::logs_sample(y, dat),
           error = function(e) NA_real_, finally = NA_real_)
}

logs_norm <- function(y, mean, sd) {
  tryCatch(scoringRules::logs_norm(y, mean = mean, sd = sd),
           error = function(e) NA_real_, finally = NA_real_)
}



globalVariables(c("crps" ,"filename",
                  "horizon", "model_id", "target_id",
                  "logs","observed", "pub_time", "start_time",
                  "predicted", "variable", "interval",
                  "statistic", "time",
                  "mean", "sd", "forecast",
                  "siteID", "plotID", "height", "depth"), package="score4cast")

