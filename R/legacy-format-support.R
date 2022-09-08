map_old_format <- function(df) {
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
  
  if ("pub_time" %in% colnames(df) &  !("start_time" %in% colnames(df))) {
    df <- df |> 
      dplyr::mutate(reference_datetime = lubridate::as_datetime(pub_time))
  }
  df
}

globalVariables(c("ensemble", "pub_time"), package="score4cast")