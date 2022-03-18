
# FIXME pivot only if `variable` column not present
# (i.e. so scoring works with new standard long-form)
# FIXME rename `target` column to `variable`

# standardizes format, 
# pivots to long form
# deduplicates predictions

#' pivot target
#' 
#' @param df standardized target data.frame in wide format
#' @param target_vars a character vector of target variable names
#' @export
pivot_target <- function(df, target_vars = ""){
  
  df %>% 
    standardize_format(target_vars = target_vars) %>% 
    tidyr::pivot_longer(tidyselect::any_of(target_vars), 
                        names_to = "target", 
                        values_to = "observed") %>% 
    filter(!is.na(observed))
}

#' pivot forecast
#' @param df standardized target data.frame in wide format
#' @param target_vars a character vector of target variable names
#' @export
pivot_forecast <- function(df, target_vars=""){
  
  df <- df %>% 
    split_filename() %>%
    standardize_format(target_vars = target_vars) %>% 
    tidyr::pivot_longer(tidyselect::any_of(target_vars), 
                        names_to = "target", 
                        values_to = "predicted")
  
  df <- deduplicate_predictions(df)
  
  if("statistic" %in% colnames(df)){
    df <- df %>% 
      tidyr::pivot_wider(names_from = statistic,
                         values_from = predicted)
  }
  
  df
}
