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
#' are retained as `crps_team` and `logs_team` columns, while
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
#' @param null_team the "team" name identifying the baseline (null) forecast
#' used for filling missing values.  
fill_scores <- function(df, null_team = "EFInull") {
  
  ## effectively assumes a single theme.
  null <- df %>% 
    filter(team == null_team) %>%
    select("theme", "target", "x","y","z", "site", "time",
           "forecast_start_time", "crps", "logs")
  all <- tidyr::expand_grid(null, distinct(df,team))
  na_filled <- dplyr::left_join(all, df,
                         by = c("theme", "team", "target", "x","y","z",
                                "site", "time", "forecast_start_time"),
                         suffix = c("_null", "_team"))
  null_filled <- na_filled %>% 
    dplyr::mutate(
    crps = dplyr::case_when(is.na(crps_team) ~ crps_null,
                     !is.na(crps_team) ~ crps_team),
    logs = dplyr::case_when(is.na(logs_team) ~ logs_null,
                     !is.na(logs_team) ~ logs_team))%>% 
    dplyr::select(-crps_null, -logs_null)
  
  null_filled
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
mean_scores <- function(df){

  df %>% 
    dplyr::group_by(team, target) %>%
    dplyr::summarise(crps = mean(crps),
              logs = mean(logs),
              sample_crps = mean(crps_team, na.rm=TRUE),
              sample_logs = mean(logs_team, na.rm=TRUE),
              percent_NA = mean(is.na(crps_team)), .groups = "drop") 
  
}


globalVariables(c("crps", "crps_self", "filled_crps", "filled_logs",
                  "forecast_start_time", "logs", "mean_crps", "null_filled_crps",
                  "null_filled_logs", "target", "team", "theme",
                  "crps_null", "logs_null"), "score4cast")
