score_schema <- arrow::schema(
  datetime = arrow::timestamp("us"), 
  family=arrow::string(),
  variable = arrow::string(), 
  prediction=arrow::float64(), 
  reference_datetime=arrow::string(),
  site_id=arrow::string(),
  model_id = arrow::string(),
  observation=arrow::float64(),
  crps = arrow::float64(),
  logs = arrow::float64(),
  mean = arrow::float64(),
  median = arrow::float64(),
  sd = arrow::float64(),
  quantile97.5 = arrow::float64(),
  quantile02.5 = arrow::float64(),
  quantile90 = arrow::float64(),
  quantile10= arrow::float64()
)




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
#' @param local_prov path to local csv file which will be used to 
#' store provenance until theme is finished scoring.
#' @export
score_theme <- function(theme, 
                        s3_forecasts, 
                        s3_targets, 
                        s3_scores, 
                        s3_prov,
                        local_prov =  paste0(theme, "-scoring-prov.csv")
){
  
  prov_download(s3_prov, local_prov)
  prov_df <- readr::read_csv(local_prov, show_col_types = FALSE)
  on.exit(prov_upload(s3_prov, local_prov))
  
  
  s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}", theme=theme))
  
  timing <- bench::bench_time({
    
    target <- get_target(theme, s3_targets)
    
    fc_path <- s3_forecasts$path(glue::glue("parquet/{theme}"))
    
    
    
    forecast_schema <- 
      arrow::schema(target_id = arrow::string(), 
                    datetime = arrow::timestamp("us"), 
                    parameter=arrow::string(),
                    variable = arrow::string(), 
                    prediction=arrow::float64(),
                    family=arrow::string(),
                    reference_datetime=arrow::string(),
                    site_id=arrow::string(),
                    model_id = arrow::string(),
                    date=arrow::string()
    )
    ## We could assemble this from s3_forecasts$ls() instead
    fc <- arrow::open_dataset(fc_path, schema=forecast_schema)
    
    grouping <- get_grouping(fc_path)
    n <- nrow(grouping)
    
    pb <- progress::progress_bar$new(
      format = glue::glue("  scoring {theme} [:bar] :percent in :elapsed,",
                          " eta: :eta"),
      total = n, 
      clear = FALSE, width= 80)  
    #for (i in 1:n) { pb$tick }
    parallel::mclapply(1:n, score_group, grouping, fc, target, prov_df,
                       local_prov, s3_scores_path, pb)

   })
  
  ## now sync prov back to S3 -- overwrites
  prov_upload(s3_prov, local_prov)
  timing
}


# ex <- score_group(1, grouping, fc, target, prov_df, local_prov, s3_scores_path, pb)

score_group <- function(i, grouping, fc, target,
                        prov_df, local_prov, s3_scores_path, pb) { 
  
  pb$tick()
  
  group <- grouping[i,]
  
  ref <- lubridate::as_datetime(group$date)
  
  tg <- target |>
    filter(datetime >= ref, datetime < ref+lubridate::days(1))
  
  ## ID changes only if target has changed between dates for this group
  id <- rlang::hash(list(group,  tg))
  
  if (!prov_has(id, prov_df)) {
    
    ## Forecast data (parquet content) is only read if it needs be scored
    ## otherwise we can skip after only looking at forecast file names (partitions).
    fc_i <- fc |> 
      dplyr::filter(model_id == group$model_id, 
                    date == group$date) |> 
      dplyr::collect()
    
    fc_i |> 
      filter(!is.na(family)) |>
      crps_logs_score(tg) |>
      mutate(date = group$date) |>
      arrow::write_dataset(s3_scores_path,
                           partitioning = c("model_id",
                                            "date"))
    prov_add(id, local_prov)
  }
}
  


get_grouping <- function(s3) {
  
  ## file-path version of:
  #  arrow::open_dataset(s3) |> dplyr::distinct(model_id, date) |>
  # dplyr::collect() 
  
  models <- stringr::str_remove(basename(s3$ls()), "model_id=")
  out <- purrr::map_dfr(models, function(model_id) {
    date <- glue::glue("model_id={model_id}") |>
      s3$ls(recursive=TRUE) |> 
      stringr::str_match("date=(\\d{4}-\\d{2}-\\d{2})")
    date <- na.omit(date[,2])
    tibble::tibble(model_id,date)
  })
  out
}


get_target <- function(theme, s3) {
  options("readr.show_progress"=FALSE)
  key <- glue::glue("{theme}/{theme}-targets.csv.gz")
  read4cast::read_forecast(key, s3 = s3)
}




## FIXME prov_has / prov_add could ideally read from & append to the *remote* S3 source.

prov_has <- function(id, prov) {
  ## would be fast even with remote file
  prov |> dplyr::filter(prov == id) |> nrow() >= 1
}

prov_add <- function(id, local_prov = "scoring_provenance.csv") {
  new_prov <-  dplyr::tibble(prov=id)
  
  ## Cannot append w/o using low-level interface
  #arrow::write_csv_arrow(new_prov, s3_prov$path(local_prov), append=TRUE)
  
  readr::write_csv(new_prov, local_prov, append=TRUE)
}

prov_download <- function(s3_prov, local_prov = "scoring_provenance.csv") {
  if(! (local_prov %in% s3_prov$ls()) ) {
    arrow::write_csv_arrow(dplyr::tibble(prov=NA), local_prov)
    return(NULL)
  }
  path <- s3_prov$path(local_prov)
  prov <- arrow::read_csv_arrow(path)
  arrow::write_csv_arrow(prov, local_prov)
  invisible(local_prov)
}

prov_upload <- function(s3_prov, local_prov = "scoring_provenance.csv") {
  prov <- arrow::open_dataset(local_prov, format="csv")
  path <- s3_prov$path(local_prov)
  prov <- arrow::write_csv_arrow(prov, path)
}


