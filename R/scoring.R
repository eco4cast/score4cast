#' Compute the CRPS & logs score of your forecast
#' 
#' @param forecast forecast data frame or file
#' @param target target path or URL (csv file)
#' @param allow_difftime Allow horizon to be expressed in difftime?
#'   Default TRUE. Otherwise will be converted into a numeric expressing
#'   difftime in seconds.  Note that using difftime is not 
#'   always compatible with other formats. Behavior can also be set
#'   globally using the option `neon4cast.allow_difftime`
#' @param ... additional parameters (Not used)
#' @importFrom dplyr `%>%` pull mutate select distinct filter 
#' @importFrom dplyr group_by summarise left_join rename
#' 
#' @details Use `[standardize_forecast]` manually if necessary first.
#' @export
score <- function(forecast, target, 
                  allow_difftime = getOption("neon4cast.allow_difftime", TRUE),
                  ...){
  
  ## read from file if necessary
  if(!inherits(forecast, "data.frame")){
    filename <- basename(forecast)
    forecast <- read4cast::read_forecast(forecast) |>
      mutate(filename = filename)
  }
  ## tables must declare theme and be in "long" form:
  if(!inherits(target, "data.frame")) {
    target <- readr::read_csv(target)
  }
  
  crps_logs_score(forecast, target) |>
    include_horizon(allow_difftime = allow_difftime)
  
}


