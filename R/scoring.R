#' Compute the CRPS & logs score of your forecast
#' 
#' @param forecast forecast data frame or file
#' @param target target path or URL (csv file)
#' @param ... additional parameters (Not used)
#' @importFrom dplyr `%>%` pull mutate select distinct filter 
#' @importFrom dplyr group_by summarise left_join rename
#' 
#' 
#' @export
score <- function(forecast, target, ...){
  
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
    include_horizon()
  
}


