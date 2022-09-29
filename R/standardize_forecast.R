

#' Transform older (v0.4) standard to current standard
#' 
#' @param df a forecast data.frame
#' @param filename, optional.
#' model_id and reference_datetime may be omitted if they are supplied in the
#' filename.  If these columns exist, then filename is ignored.
#' @details
#' Current standard should have columns:
#' - model_id
#' - reference_datetime
#' - site_id
#' - variable
#' - datetime
#' - family
#' - parameter
#' - predicted
#' 
#' This function does not handle un-pivoted (v0.3) forecast, see pivot_forecast()
#' 
#' @export
standardize_forecast <- function(df, filename=NULL) {

  if ("ensemble" %in% colnames(df)) {
    df <- df |>
      dplyr::mutate(family = "sample") |> 
      dplyr::mutate(ensemble = as.character(ensemble)) |>
      dplyr::rename(parameter = ensemble)
  } else if ("mean" %in% colnames(df)) {
    df <- df |>
      tidyr::pivot_longer(dplyr::any_of(c("mean", "sd")),
                          names_to = "parameter", 
                          values_to = "predicted") |>
      dplyr::mutate(family="normal",
                    parameter=forcats::fct_recode(parameter,
                                                  mu="mean", 
                                                  sigma="sd"))
    
  } else if ("statistic" %in% colnames(df)) {
    df <- df |> 
      dplyr::rename(parameter = "statistic") |>
      dplyr::mutate(family="normal")
  }
  
  if( nrow( dplyr::filter(df, parameter == "mean") ) > 0) {
    df <- df |>  dplyr::mutate(parameter=forcats::fct_recode(parameter,
                                                             mu="mean",
                                                             sigma="sd"))
  }

  ## can only recode if factor is actually used:
  if( nrow( dplyr::filter(df, family == "ensemble") ) > 0) {
    df <- df |> dplyr::mutate(family = 
                               forcats::fct_recode(family,
                                                   sample="ensemble")
    )
  }

  
  
  ##
  if ("pub_time" %in% colnames(df) && ! "reference_datetime" %in% colnames(df)) {
    df <- df |> rename(reference_datetime = pub_time)
  }
  if ("start_time" %in% colnames(df) && ! "reference_datetime" %in% colnames(df)) {
    df <- df |> rename(reference_datetime = start_time)
  }
  if ("start_time" %in% colnames(df)) {
    df  <- df |> select(-dplyr::any_of("start_time")) # now drow old name
  }
  
  if ("pub_time" %in% colnames(df)) { # don't need this, keeps things simple/standard
    df  <- df |> select(-dplyr::any_of("pub_time"))
  }
  
  if("time" %in% colnames(df)) {
    df <- df |> rename(datetime = time)
  }

  
  if ("filename" %in% colnames(df)) {
    filename <- df |> pull(filename)
    df <- df |> select(-filename)
  }
  
  ## Only add these if not present
  if(!is.null(filename)) {
    pattern <- 
      "(\\w+)\\-(\\d{4}\\-\\d{2}\\-\\d{2})\\-(\\w+)\\.(csv)?(\\.gz)?(nc)?"
    x <- basename(filename)
    
    
    #if (!"target_id" %in% colnames(df)) 
    #  df <- df %>% mutate(target_id = gsub(pattern, "\\1", x))
    
    if (!"reference_datetime" %in% colnames(df)) {
      df <- mutate(df, reference_datetime = gsub(pattern,"\\2", x))
    }

    if (!"model_id" %in% colnames(df)) {
      df <- df |> mutate(model_id = gsub(pattern, "\\3", x))
    }
    
  }
  
  ## Some tick counts are predicted as integer (ensemble), but not always (parametric).
  ## for consistent typing, always treat this field as numeric
  if(inherits(df$predicted, "integer")) {
    df <- df |> mutate(predicted = as.numeric(predicted))
  }

  ## ensemble number should not be an integer/factor,
  ## as that conflicts with parametric parameter names
  df <- df |> mutate(parameter = as.character(parameter))
  
  ## Enforce ISO vars for ticks or beetles vars
  ## (Assumes `df` doesn't include forecasts from different themes!)
  iso_vars <- c("abundance", "richness", "amblyomma_americanum")
  if ( nrow( dplyr::filter(df, variable %in%  iso_vars ) ) > 0 ) {
    df <- df |> mutate(datetime = isoweek(datetime))
  }
  
  original <- getOption("digits.sec")
  options("digits.secs"=0)
  df <- df |> mutate(reference_datetime = as.character(reference_datetime))
  options("digits.secs"=original)
    
  df
  
}

globalVariables(c("ensemble", "filename", "pub_time", "start_time", "time"),
                package="score4cast")
