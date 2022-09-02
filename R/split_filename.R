## Parses neon4cast challenge forecast filename components.
split_filename <- function(df){
  ## arguably better to split on "-" and unite date components?
  if("filename" %in% colnames(df)) {
    pattern <- "(\\w+)\\-(\\d{4}\\-\\d{2}\\-\\d{2})\\-(\\w+)\\.(csv)?(\\.gz)?(nc)?"
    df <- df %>% 
      mutate(target_id = gsub(pattern, "\\1", basename(filename)),
             pub_time = gsub(pattern, "\\2", basename(filename)),
             model_id = gsub(pattern, "\\3", basename(filename)))
  }
  df
}
globalVariables(c("filename"), package="score4cast")
