


fable_to_efi <- function(df) {
  
  ## do one model at a time.
  ## How do we handle transformed cases?
  
  dt <- tsibble::index(df)
  predicted <- rlang::sym( fabletools::response_vars(df) )
  keys <- tsibble::key(df)
  
  df2 <- df |>
    tibble::as_tibble() |> 
    dplyr::mutate( datetime = lubridate::as_datetime({{dt}})) |>
    dplyr::mutate( family = family({{predicted}}) ) |>
    group_by(family, datetime, {{predicted}}, across(as.character(keys))) |>
    dplyr::summarise(parameter = 
                       names(distributional::parameters({{predicted}})),
                     predicted =
                       unlist(distributional::parameters({{predicted}})),
                     .groups = "drop") |>
    dplyr::select(any_of(c("datetime", as.character(keys),
                           "family", "parameter", "predicted")))
    
    df2
    
}

## Note, there is still an additional case of generating an ensemble from from
## parameteric fable.  Not ideal but currently necessary if distribution is not
## supported; e.g. required for transformed distributionals currently...

