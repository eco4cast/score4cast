map_old_format <- function(df, filename=NULL) {
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

  ##
  if ("pub_time" %in% colnames(fc)) {
    df <- df |> rename(reference_datetime = pub_time)
  }
  if ("start_time" %in% colnames(fc)) {
    df <- df |> rename(reference_datetime = start_time)
  }
  if ("start_time" %in% colnames(fc)) {
    df  <- df |> select(-start_time)
  }
  if("time" %in% colnames(fc)) {
    df <- df |> rename(datetime = time)
  }

  
  if ("filename" %in% colnames(df)) {
    filename <- df |> pull(filename)
    df <- df |> select(-filename)
  }
  
  if(!is.null(filename)) {
    pattern <- 
      "(\\w+)\\-(\\d{4}\\-\\d{2}\\-\\d{2})\\-(\\w+)\\.(csv)?(\\.gz)?(nc)?"
    x <- basename(filename)
    df <- df %>% 
      mutate(target_id = gsub(pattern, "\\1", x),
             reference_datetime =
               lubridate::as_datetime(gsub(pattern, "\\2", x)),
             model_id = gsub(pattern, "\\3", x))
  }
  df
  
}

globalVariables(c("ensemble", "pub_time", "start_time", "filename"),
                package="score4cast")