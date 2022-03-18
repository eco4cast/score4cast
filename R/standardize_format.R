
## Tidy date formats and drop non-standard columns
## shared by targets + forecasts
standardize_format <- function(df, target_vars) {
  
  renamer <- function(x) {
    vapply(x, function(n) switch(n,
                                "target" = "variable",
                                "siteID" = "site",
                                "latitude" = "y",
                                "lognitude" = "x",
                                "depth" = "z",
                                "height" = "z",
                                "theme"  = "project",
                                "team"   = "model",
                                n),
           "", USE.NAMES = FALSE)
  }
  df <- dplyr::rename_with(df,renamer)
  
  column_names <- c("project", "model",
                    "issue_date", "site", "x", "y", "z", "time",
                    "variable", "ensemble", "statistic")
  #GENERALIZATION:  This is a theme specific hack. How do we generalize?
  ## Put tick + beetles dates to ISOweek
  if ("project" %in% colnames(df) && 
      all(pull(df,project) %in% c("ticks", "beetles"))
      ) {
    df <- df %>% 
      mutate(time = isoweek(time))
    if("plotID" %in% names(df)) {
      df <- df %>% 
        select(-siteID) %>%
        rename(site = plotID)
    }
  }
  
  if(!("site" %in% colnames(df))){
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
      mutate(project = gsub(pattern, "\\1", basename(filename)),
             issue_date = gsub(pattern, "\\2", basename(filename)),
             forecst_model_id = gsub(pattern, "\\3", basename(filename)))
  }
  df
}

## utils 
isoweek <- function(time) { # Note: ISOweek calls not duckdb-compatible
  ISOweek::ISOweek2date(paste0(ISOweek::ISOweek(time), "-","1"))
}
na_rm <- function(x) as.numeric(stats::na.exclude(x))
