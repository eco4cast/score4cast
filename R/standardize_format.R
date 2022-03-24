
## Tidy date formats and drop non-standard columns
## shared by targets + forecasts
standardize_format <- function(df, target_vars) {
  
  renamer <- function(x) {
    vapply(x, function(n) switch(n,
                                "target" = "variable",
                                "siteID" = "site_id",
                                "site"   = "site_id",
                                "latitude" = "y",
                                "lognitude" = "x",
                                "depth" = "z",
                                "height" = "z",
                                "theme"  = "target_id",
                                "team"   = "model_id",
                                "forecast_start_time" = "start_time",
                                "issue_date" = "pub_time",
                                "Amblyomma americanum" = "amblyomma_americanum",
                                n),
           "", USE.NAMES = FALSE)
  }
  df <- dplyr::rename_with(df,renamer)
  
  column_names <- c("target_id", "model_id", "start_time",
                    "pub_time", "site_id", "x", "y", "z", "time",
                    "variable", "ensemble", "statistic", target_vars)
  
  
  #GENERALIZATION:  This is a theme specific hack. How do we generalize?
  ## Put tick + beetles dates to ISOweek
  if ("target_id" %in% colnames(df) && 
      all(pull(df,target_id) %in% c("ticks", "beetles"))
      ) {
    df <- df %>% 
      mutate(time = isoweek(time))
    if("plotID" %in% names(df)) {
      df <- df %>% 
        select(-any_of("site_id")) %>%
        rename(site_id = plotID)
    }
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
    mutate(across(any_of(c("time", "start_time")),
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
      mutate(target_id = gsub(pattern, "\\1", basename(filename)),
             pub_time = gsub(pattern, "\\2", basename(filename)),
             model_id = gsub(pattern, "\\3", basename(filename)))
  }
  df
}

## utils 
isoweek <- function(time) { # Note: ISOweek calls not duckdb-compatible
  ISOweek::ISOweek2date(paste0(ISOweek::ISOweek(time), "-","1"))
}
na_rm <- function(x) as.numeric(stats::na.exclude(x))
