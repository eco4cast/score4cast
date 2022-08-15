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
      dplyr::mutate(family="norm")
    
  } else if ("statistic" %in% colnames(df)) {
    df <- df |> 
      dplyr::rename(parameter = "statistic") |>
      dplyr::mutate(family="norm")
  }
  
  
  if (!("start_time" %in% colnames(df))) {
    df <- df |> 
      dplyr::mutate(start_time = lubridate::as_datetime(pub_time))
  }
  
  
  df
}