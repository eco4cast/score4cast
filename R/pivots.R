
# Deprecated Legacy support for unpivoted forecasts, v0.3

#' pivot target
#' 
#' @param df standardized target data.frame in wide format
#' @param target_vars a character vector of target variable names
#' @export
pivot_target <- function(df, target_vars = NULL){
  df <- df %>% 
    standardize_format(target_vars = target_vars) 
  if (!("variable" %in% colnames(df))) {
    df <- df %>% 
    tidyr::pivot_longer(tidyselect::any_of(target_vars), 
                        names_to = "variable", 
                        values_to = "observation")
  }
  df %>% filter(!is.na(observation))
}

#' pivot forecast
#' @param df standardized target data.frame in wide format
#' @param target_vars a character vector of target variable names
#' @export
pivot_forecast <- function(df, target_vars=""){
  
  df <- df %>% 
    split_filename() %>%
    standardize_format(target_vars = target_vars)
  if (!("variable" %in% colnames(df))) {
    df <- df %>% 
      tidyr::pivot_longer(tidyselect::any_of(target_vars), 
                          names_to = "variable", 
                          values_to = "prediction")
  }
  df <- deduplicate_predictions(df)
  
  if("statistic" %in% colnames(df)){
    df <- df %>% 
      tidyr::pivot_wider(names_from = statistic,
                         values_from = prediction)
  }
  
  df
}


## deprecated support for un-pivoted forecasts, standards version 0.3
standardize_format <- function(df, target_vars) {
  
  
  renamer <- function(x) {
    vapply(x,
           function(n)
             switch(
               n,
               "start_time" = "reference_datetime",
               "time" = "datetime",
               "target" = "variable",
               "siteID" = "site_id",
               "site"   = "site_id",
               "theme"  = "target_id",
               "team"   = "model_id",
               "forecast_start_time" = "reference_datetime",
               "issue_date" = "pub_time",
               "Amblyomma americanum" = "amblyomma_americanum",
               n
             ),
           "", USE.NAMES = FALSE)
  }
  df <- dplyr::rename_with(df,renamer)
  
  
  column_names <- c("target_id",
                    "model_id", 
                    "reference_datetime",
                    "site_id", 
                    "datetime",
                    "variable",
                    "family", 
                    "parameter",
                    "prediction",
                    "observation",
                    ## And deprecated names
                    "ensemble",
                    "statistic",
                    "mean",
                    "sd"
                    )
  
  
  #GENERALIZATION:  This is a theme specific hack. How do we generalize?
  ## Put tick + beetles dates to ISOweek
  if ("target_id" %in% colnames(df) && 
      all(pull(df,target_id) %in% c("ticks", "beetles"))
  ) {
    df <- df %>% 
      mutate(datetime = isoweek(datetime))
  }
  
  if (!"observed" %in% colnames(df)) {
    df <- df |> rename(observation = observed)
  }
  
  # drop non-standard columns
  df %>% 
    dplyr::select(tidyselect::any_of(column_names)) %>%
    enforce_schema()
}

enforce_schema <- function(df) {
  df %>% 
    mutate(across(any_of(c("datetime", "reference_datetime")),
                  .fns = as.POSIXct)) %>%
    mutate(prediction = as.numeric(prediction),
           parameter = as.character(parameter))
}


## utils 
isoweek <- function(datetime) { # Note: ISOweek calls not duckdb-compatible
  ISOweek::ISOweek2date(paste0(ISOweek::ISOweek(datetime), "-","1"))
}
na_rm <- function(x) as.numeric(stats::na.exclude(x))



globalVariables(c("target_id", "datetime", "prediction", "parameter",
                  "statistic"),
                package="score4cast")




