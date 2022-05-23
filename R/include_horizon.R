#' include_horizon
#' 
#' @param df data.frame containing an efi-compliant forecast or score
#' @param allow_difftime should `horizon` and `interval` be expressed as integers (in seconds)
#' (default) or as native difftime (compatible in R but not in serializations like csv or parquet)
#' @export
include_horizon <- function(df, 
                            allow_difftime = getOption("neon4cast.allow_difftime", FALSE)){
  
  interval <- df %>%
    group_by(across(any_of(c("target_id", "model_id", "pub_time", 
                             "variable", "site_id")))) %>% 
    summarise(interval = min(time - dplyr::lag(time), na.rm=TRUE),
              start_time = min(time), #- interval,
              .groups = "drop")
  
  ## add columns for start_time and horizon
  df <- df %>% 
    left_join(interval,
              #by = c("target_id", "model_id", "pub_time", "site_id", "variable")
              ) %>% 
    mutate(horizon = time - start_time)
  
  if(!allow_difftime){
    df <- df %>% mutate(horizon = as.numeric(lubridate::as.duration(horizon), units="seconds"),
                        interval = as.numeric(lubridate::as.duration(interval), units="seconds"))
  }
  df
}


