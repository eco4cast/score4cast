#' include_horizon
#' 
#' Adds horizon if not present, defined as `datetime - reference_datetime` with a group
#' defined by any of: "target_id", "model_id", "pub_time", "variable", "site_id".
#' Will also add `reference_datetime` if not present, defined as `datetime - min(datetime)`
#' @param df data.frame containing an efi-compliant forecast or score
#' @param allow_difftime should `horizon` and `interval` be expressed as integers (in seconds)
#' (default) or as native difftime (compatible in R but not in serializations like csv or parquet)
#' @export
include_horizon <- function(df, 
                            allow_difftime = 
                              getOption("neon4cast.allow_difftime",
                                        FALSE)){
  if (!"reference_datetime" %in% colnames(df)) {
    interval <- df %>%
      group_by(across(any_of(c("model_id", "reference_datetime", 
                               "variable", "site_id")))) %>% 
      summarise(reference_datetime = min(datetime), #- interval,
                .groups = "drop")
    
    df <- df %>%
      left_join(interval)
  }
  ## add columns for start_time and horizon
  if(!"horizon" %in% colnames(df)) {
    df <- df %>%
      mutate(horizon = 
               lubridate::as_datetime(datetime) -
               lubridate::as_datetime(reference_datetime))
  }
  if(!allow_difftime){
    df <- df %>% 
      mutate(horizon = as.numeric(lubridate::as.duration(horizon),
                                  units="seconds"))
  }
  df
}

globalVariables(c("datetime", "reference_datetime", "horizon"), package="score4cast")

