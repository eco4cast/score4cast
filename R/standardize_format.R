
## Tidy date formats and drop non-standard columns
## shared by targets + forecasts
standardize_format <- function(df, target_vars) {
  
  renamer <- function(x) {
    vapply(x, function(n) switch(n,
                                "target" = "variable",
                                "siteID" = "site_id",
                                "site"   = "site_id",
                                "theme"  = "target_id",
                                "team"   = "model_id",
                                "forecast_start_time" = "reference_datetime",
                                "issue_date" = "pub_time",
                                "Amblyomma americanum" = "amblyomma_americanum",
                                n),
           "", USE.NAMES = FALSE)
  }
  df <- dplyr::rename_with(df,renamer)
  
  deprecrated_names <- c(target_vars,
                         "target_id", 
                         "ensemble", 
                         "statistic", 
                         "mean",
                         "sd")
  
  column_names <- c(deprecrated_names,
                    "family", "parameter",
                    "model_id", "reference_datetime",
                    "site_id", "datetime",
                    "variable", 
                    "predicted", "observed"
                    )
  
  
  #GENERALIZATION:  This is a theme specific hack. How do we generalize?
  ## Put tick + beetles dates to ISOweek
  if ("target_id" %in% colnames(df) && 
      all(pull(df,target_id) %in% c("ticks", "beetles"))
      ) {
    df <- df %>% 
      mutate(datetime = isoweek(datetime))
    
### DEPRECATATED ticks pools up to siteID instead
#    if("plotID" %in% names(df)) {
#      df <- df %>% 
#        select(-any_of("site_id")) %>%
#        rename(site_id = plotID)
#    }
  }
  
  
  
  if(!("site_id" %in% colnames(df))){
    df <- dplyr::mutate(df, site = NA)
  }
  
  if(!("z" %in% colnames(df))){
    df <- dplyr::mutate(df, z = NA)
  }
  
  if(!("y" %in% colnames(df))){
    df <- dplyr::mutate(df, y = NA)
  }
  
  if(!("x" %in% colnames(df))){
    df <- dplyr::mutate(df, x = NA)
  }
  
  # drop non-standard columns
  df %>% 
    dplyr::select(tidyselect::any_of(column_names)) %>%
    enforce_schema()
}

enforce_schema <- function(df) {
  df %>% 
    mutate(across(any_of(c("datetime", "reference_datetime")),
                  .fns = as.POSIXct))
}


#Select forecasted times using "forecast" flag in standard
select_forecasts <- function(df, only_forecasts){
  
  if("forecast" %in% colnames(df) & only_forecasts){
    df <- df %>% dplyr::filter(forecast == 1)
  }
  df
}



## utils 
isoweek <- function(datetime) { # Note: ISOweek calls not duckdb-compatible
  ISOweek::ISOweek2date(paste0(ISOweek::ISOweek(datetime), "-","1"))
}
na_rm <- function(x) as.numeric(stats::na.exclude(x))





globalVariables(c("target_id", "datetime", "forecast"), package="score4cast")
