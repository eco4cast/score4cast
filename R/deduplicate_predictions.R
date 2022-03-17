
#' @importFrom dplyr across any_of

deduplicate_predictions <- function(df){
  
  has_dups <- df %>% 
    select(-any_of("predicted")) %>% 
    vctrs::vec_group_id() %>% 
    vctrs::vec_duplicate_any()
  
  if(has_dups) {
    df <- df %>%
      filter(!is.na(predicted)) %>%
      group_by(across(-any_of("predicted"))) %>% 
      filter(dplyr::row_number() == 1L)
  }
  
  df
}
