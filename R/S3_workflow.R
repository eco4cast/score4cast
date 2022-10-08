

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
#' @param max_horizon days after the reference_date that a given forecast should cover.
#' Can exceed the actual max horizon. Allows us to avoid re-scoring old forecasts when
#' targets are updated with future values but leave old values unchanged.
#' @param local_prov path to local csv file which will be used to 
#' store provenance until theme is finished scoring.
#' @export
score_theme <- function(theme, 
                        s3_forecasts, 
                        s3_targets, 
                        s3_scores, 
                        s3_prov,
                        max_horizon = 365L,
                        local_prov =  paste0(theme, "-scoring-prov.csv")
                        ){
  
  prov_download(s3_prov, local_prov)
  prov_df <- readr::read_csv(local_prov, show_col_types = FALSE)
  on.exit(prov_upload(s3_prov, local_prov))
  
  
  s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}", theme=theme))
  
  timing <- bench::bench_time({
    
  target <- get_target(theme, s3_targets)
  fc <- arrow::open_dataset(s3_forecasts$path(glue::glue("parquet/{theme}"))) 

  ## We will score in group chunks (model/date/site) to save RAM
  ## Computing group list can be pretty slow when many files are involved!
  ## does not seem to be leveraging partition names!
  grouping <- fc |> 
    dplyr::distinct(model_id, reference_datetime, site_id) |>
    dplyr::collect()
  n <- nrow(grouping)
  
  pb <- progress::progress_bar$new(
    format = glue::glue("  scoring {theme} [:bar] :percent in :elapsed,",
                        " eta: :eta"),
    total = n, 
    clear = FALSE, width= 80)  
  for (i in 1:n) {

    pb$tick()
    group <- grouping[i,]

    ref <- lubridate::as_date(group$reference_datetime)
    bounds <- list(min = ref, max = ref + max_horizon)
    
    tg <- target |>
      filter(datetime >= bounds$min,
             datetime <= bounds$max)

    ## ID changes only if target has changed between dates for this group
    id <- rlang::hash(list(group$model_id, 
                           group$reference_datetime,
                           group$site_id,
                           tg))
    
    if (!prov_has(id, prov_df)) {
      
    ## Forecast data (parquet content) is only read if it needs be scored
    ## otherwise we can skip after only looking at forecast file names (partitions).
      fc_i <- fc |> 
        dplyr::filter(model_id == group$model_id, 
                      reference_datetime == group$reference_datetime,
                      site_id == group$site_id) |> 
        dplyr::collect()
      
      fc_i |> 
        filter(!is.na(family)) |>
        crps_logs_score(tg) |>
        arrow::write_dataset(s3_scores_path,
                             partitioning = c("model_id",
                                              "reference_datetime",
                                              "site_id"))
      prov_add(id, local_prov)
    }

  }
  
 })
  
  ## hack to merge prov with any updates to prov posted while we were running.
  #prov_download(s3_prov, "tmp.csv")
  #dplyr::bind_rows(readr::read_csv("tmp.csv", show_col_types = FALSE),
  #                 readr::read_csv(local_prov, show_col_types = FALSE)) |>
  #  dplyr::distinct() |>
  #readr::write_csv(local_prov)
  
  ## now sync prov back to S3
  prov_upload(s3_prov, local_prov)
  timing
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


