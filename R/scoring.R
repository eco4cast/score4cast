#' Compute the CRPS & logs score of your forecast
#' 
#' @param forecast forecast data frame or file
#' @param target target path or URL (csv file)
#' @param theme a theme name, an additional metadata field
#' which is attached as an extra column if not available.  Serves as a forecast series identifier
#' @param target_vars for 'wide' format, character vector of valid variable names
#' @importFrom dplyr `%>%` pull mutate select distinct filter 
#' @importFrom dplyr `%>%` group_by summarise left_join rename
#' 
#' 
#' @export
score <- function(forecast,
                  target,
                  theme = c("aquatics", "beetles",
                           "phenology", "terrestrial_30min",
                           "terrestrial_daily","ticks"),
                  target_vars = c("oxygen", 
                                  "temperature", 
                                  "richness",
                                  "abundance", 
                                  "nee",
                                  "le", 
                                  "vswc",
                                  "gcc_90",
                                  "rcc_90",
                                  "ixodes_scapularis",
                                  "amblyomma_americanum",
                                  "Amblyomma americanum")){
  
  theme = match.arg(theme)
  
  ## read from file if necessary
  if(is.character(forecast)){
    filename <- forecast
    forecast <- read_forecast(forecast) %>% 
      mutate(filename = filename)
  }
  ## tables must declare theme and be in "long" form:
  target <- readr::read_csv(target) %>% 
    dplyr::mutate(theme = theme) %>%
    pivot_target(target_vars)
  
  forecast <- forecast %>% 
    dplyr::mutate(theme=theme) %>%
    pivot_forecast(target_vars)
  
  crps_logs_score(forecast, target)
  
}


