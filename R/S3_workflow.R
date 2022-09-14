

#' score_theme
#' 
#' A helper utility to score a collection of forecasts efficiently
#' for challenges hosting submitted forecasts on S3 buckets.
#' Scores are automatically streamed to an "scores" bucket in parquet format.
#' A provenance bucket is used to allow function to skip forecast-target
#' combinations that have already been scored.
#' 
#' @param theme which theme should be scored?
#' @param s3_forecasts a connection from [arrow::s3_bucket]
#' @param s3_targets a connection from [arrow::s3_bucket]
#' @param s3_scores a connection from [arrow::s3_bucket] where scores will be written.
#' This connection requires write access, e.g. by specifying 
#  AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY env vars.
#' @param s3_prov a connection from [arrow::s3_bucket]
#' @param after a date by which to filter out older forecasts from (re)-scoring
#' @param local_prov path to local csv file which will be used to 
#' store provenance until theme is finished scoring.
#' @export
score_theme <- function(theme, 
                        s3_forecasts, 
                        s3_targets, 
                        s3_scores, 
                        s3_prov, 
                        after = as.Date("2022-01-01"),
                        local_prov = "scoring_provenance.csv"){
  
  #prov_download(s3_prov, local_prov)
  #prov_df <- readr::read_csv(local_prov, show_col_types = FALSE)
  
  options("readr.show_progress"=FALSE)
  target <- get_target(theme, s3_targets)
  
  fc <- open_dataset(s3_forecasts$path(glue::glue("parquet/{theme}"))) 

  ## We will score in group chunks (model/date/site) to save RAM
  grouping <- fc |> 
    distinct(model_id, reference_datetime, site_id) |>
    collect()
  n <- nrow(grouping)
  
  for (i in 1:n) {  
    
    group <- grouping[i,]
    score <- fc |> 
      filter(model_id == group$model_id, 
             reference_datetime == group$reference_datetime,
             site_id == group$site_id) |> 
      collect() |> 
      crps_logs_score(target)
    
    write_dataset(score, s3_scores,
                  partitioning=c("model_id", "reference_datetime", "site_id"))
  }
  
  
}

get_target <- function(theme, s3) {
  key <- glue::glue("{theme}/{theme}-targets.csv.gz")
  read4cast::read_forecast(key, s3 = s3)
}











prov_download <- function(s3_prov, local_prov = "scoring_provenance.csv") {
  if(!"scoring_provenance.csv" %in% s3_prov$ls() ) {
    arrow::write_csv_arrow(dplyr::tibble(prov=NA),"scoring_provenance.csv")
    return(NULL)
  }
  path <- s3_prov$path("scoring_provenance.csv")
  prov <- arrow::read_csv_arrow(path)
  arrow::write_csv_arrow(prov, local_prov)
}

prov_upload <- function(s3_prov, local_prov = "scoring_provenance.csv") {
  prov <- arrow::open_dataset(local_prov, format="csv")
  path <- s3_prov$path("scoring_provenance.csv")
  prov <- arrow::write_csv_arrow(prov, path)
}
