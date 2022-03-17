
## Tidy date formats and drop non-standard columns
## shared by targets + forecasts
standardize_format <- function(df, target_vars) {
  
  column_names <- c("theme", "team", "issue_date", "site", "x", "y", "z", "time",
                    target_vars, "ensemble", "statistic")
  
  #GENERALIZATION:  This is a theme specific hack. How do we generalize?
  ## Put tick dates to ISOweek
  ## (arguably should be applied to beetles if not already done too)
  if ("theme" %in% colnames(df) && all(pull(df,theme) == "ticks")) {
    df <- df %>% 
      mutate(time = isoweek(time))
    if("plotID" %in% names(df)) {
      df <- df %>% 
        select(-siteID) %>%
        rename(siteID = plotID)
    }
  }
  
  if(("siteID" %in% colnames(df))){
    df <- df %>% 
      rename(site = siteID)
  }
  
  if(!("site" %in% colnames(df))){
    df <- df %>% 
      mutate(site = NA)
  }
  
  if(("depth" %in% colnames(df))){
    df <- df %>% 
      rename(z = depth)
  }
  
  if(("height" %in% colnames(df))){
    df <- df %>% 
      rename(z = height)
  }
  
  
  if(!("z" %in% colnames(df))){
    df <- df %>% 
      mutate(z = NA)
  }
  
  if(("latitude" %in% colnames(df))){
    df <- df %>% 
      rename(y = latitude)
  }
  
  if(!("y" %in% colnames(df))){
    df <- df %>% 
      mutate(y = NA)
  }
  
  if(("longitude" %in% colnames(df))){
    df <- df %>% 
      rename(x = longitude)
  }
  
  if(!("x" %in% colnames(df))){
    df <- df %>% 
      mutate(x = NA)
  }
  
  # drop non-standard columns
  df %>% 
    dplyr::select(tidyselect::any_of(column_names)) %>%
    enforce_schema()
}

enforce_schema <- function(df) {
  df %>% 
    mutate(across(any_of(c("time", "forecast_start_time")),
                  .fns = as.POSIXct))
}


#Select forecasted times using "forecast" flag in standard
select_forecasts <- function(df, only_forecasts){
  
  if("forecast" %in% colnames(df) & only_forecasts){
    df <- df %>% dplyr::filter(forecast == 1)
  }
  df
}

## Parses neon4cast challenge forecast filename components.
split_filename <- function(df){
  ## arguably better to split on "-" and unite date components?
  if("filename" %in% colnames(df)) {
    pattern <- "(\\w+)\\-(\\d{4}\\-\\d{2}\\-\\d{2})\\-(\\w+)\\.(csv)?(\\.gz)?(nc)?"
    df <- df %>% 
      mutate(theme = gsub(pattern, "\\1", basename(filename)),
             issue_date = gsub(pattern, "\\2", basename(filename)),
             team = gsub(pattern, "\\3", basename(filename)))
  }
  df
}

## utils 
isoweek <- function(time) { # Note: ISOweek calls not duckdb-compatible
  ISOweek::ISOweek2date(paste0(ISOweek::ISOweek(time), "-","1"))
}
na_rm <- function(x) as.numeric(stats::na.exclude(x))
