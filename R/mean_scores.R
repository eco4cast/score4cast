#' fill scores based on a null
#' 
#' Comparing mean scores across teams requires appropriate 
#' treatment of missing values: teams should not be able to improve 
#' scores merely by refusing to provide predictions of, e.g. sites
#' or times which are hardest to predict.  To avoid this, 
#' merely removing missing values when averaging across scores is not
#' sufficient.  A simple expedient is to replace missing values with
#' predictions made from a baseline 'null' forecast.  This function
#' simply provides this behavior.  Original forecast scores with missing values 
#' are retained as `crps_model` and `logs_model` columns, while
#' `crps` and `logs` become filled with baseline scores from the null forecast.
#' 
#' Note that this fills _implicit_ NAs, e.g. site/time/variables predicted
#' by the null team but not predicted by the focal team, as well as explicit
#' `NA`s in the focal team (where the focal team includes each of teams named
#' in the teams column of `df`). 
#' If teams have scores for site/time/variables not forecasted by the "null" team,
#' these rows are removed and thus cannot contribute to the mean score either.
#' If the "null" team contains explicit `NA`s in scores,
#' (usually but not always due to missing observations), these are not removed.
#' 
#' @param df a data frame of forecasts, with column 
#' "team" identifying different forecasts.  
#' @param null_model the "team" name identifying the baseline (null) forecast
#' used for filling missing values.
#' @importFrom dplyr select collect distinct filter case_when mutate
#' @importFrom dplyr left_join pull
#' @export
fill_scores <- function(df, null_model = "EFInull") {
  df <- df %>% filter(!is.na(observed)) %>% collect()
  
  team <- distinct(df,model_id)
  if (is.na(null_model)) {
    x <- pull(team,model_id)
    null_model <- x[grepl("null", x)]
  }
  
  null <- df %>%
    filter(model_id == null_model) %>%
    select("target_id", "variable", "x","y","z", "site_id", "time",
           "start_time", "crps", "logs")
  all <- tidyr::expand_grid(null, team)
  na_filled <- left_join(all, df,
                         by = c("target_id", "model_id", "variable", "x","y","z",
                                "site_id", "time", "start_time"),
                         suffix = c("_null", "_model"))
  null_filled <- na_filled %>% mutate(
    crps = case_when(is.na(crps_model) ~ crps_null,
                     !is.na(crps_model) ~ crps_model),
    logs = case_when(is.na(logs_model) ~ logs_null,
                     !is.na(logs_model) ~ logs_model)) %>%
    select(-crps_null, -logs_null)
  
  ## express difftimes in days, not seconds
  null_filled %>% mutate(interval = as.numeric(interval, units="days"),
                         horizon = as.numeric(horizon, units="days"))
  
}


#' mean_scores
#' 
#' Compute the mean scores from [fill_scores()] table.
#' By default, results are sorted by mean_crps,
#' based on self-fill and then null-fill.  For comparisons 
#' across different horizons, scores on null_filled_crps alone
#' (no self-fill step) may be preferred.  
#' The number of missing values filled in for each forecast is also reported.
#' @param df a data frame from fill_scores()
#' @export
mean_scores <- function(df){

  df %>% 
    dplyr::group_by(model_id, variable) %>%
    dplyr::summarise(crps = mean(crps),
              logs = mean(logs),
              sample_crps = mean(crps_model, na.rm=TRUE),
              sample_logs = mean(logs_model, na.rm=TRUE),
              percent_NA = mean(is.na(crps_model)),
              .groups = "drop") 
  
}


globalVariables(c("crps_null", "logs_null", 
                  "crps_model", "logs_model"), "score4cast")
