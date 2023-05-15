

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
#' @param endpoint endpoint must be passed explicitly for s3_forecast bucket
#' @param s3_inv parquet-based  S3 inventory of forecast filepaths
#' @export
score_theme <- function(theme, 
                        s3_forecasts, 
                        s3_targets, 
                        s3_scores, 
                        s3_prov,
                        local_prov =  paste0(theme, "-scoring-prov.csv"),
                        endpoint = "data.ecoforecast.org",
                        s3_inv = arrow::s3_bucket("neon4cast-inventory", 
                                                   endpoint_override = endpoint)
){
  
  prov_download(s3_prov, local_prov)
  prov_df <- readr::read_csv(local_prov, col_types = "cc")
  on.exit(prov_upload(s3_prov, local_prov))
  
  s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}", theme=theme))
  bucket <- "neon4cast-forecasts/"
  
  timing <- bench::bench_time({
    target <- get_target(theme, s3_targets)
    grouping <- get_grouping(s3_inv, theme,  collapse=TRUE, endpoint=endpoint)
    pb <- progress::progress_bar$new(format=
      glue::glue("  scoring {theme} [:bar] :percent in :elapsed, eta: :eta"),
      total = nrow(grouping), clear = FALSE, width= 80)
    parallel::mclapply(1:n, score_group, grouping, bucket, target, prov_df,
                       local_prov, s3_scores_path, pb, endpoint)

   })
  ## now sync prov back to S3 -- overwrites
  prov_upload(s3_prov, local_prov)
  timing
}


# ex <- score_group(1, grouping, fc, target, prov_df, local_prov, s3_scores_path, pb)

score_group <- function(i, grouping, 
                        bucket, target, prov_df, local_prov,
                        s3_scores_path, pb, endpoint) { 
  pb$tick()
  group <- grouping[i,]
  ref <- lubridate::as_datetime(group$date)
  
  # NOTE: we cannot 'prefilter' grouping by prov, since once we have tg
  # we want to use it to score, not access it twice...
  tg <- target |>
    filter(datetime >= ref, datetime < ref+lubridate::days(1))
  
  id <- rlang::hash(list(grouping[i, c("model_id", "date")],  tg))
  new_id <- rlang::hash(list(group,  tg))

  if ( !(prov_has(id, prov_df, "prov") || 
         prov_has(new_id, prov_df, "new_id")) ) 
  {
    fc <- get_fcst_arrow(endpoint, bucket, theme, group)
    fc |> 
      filter(!is.na(family)) |> #hhhmmmm? what should we be doing about these forecasts?
      crps_logs_score(tg) |>
      mutate(date = group$date) |>
      arrow::write_dataset(s3_scores_path,
                           partitioning = c("model_id", "date"))
    prov_add(new_id, local_prov)
  }
}

get_grouping <- function(s3_inv, theme,
                         collapse=TRUE, 
                         endpoint="data.ecoforecast.org") {
  
  groups <- arrow::open_dataset(s3_inv$path("neon4cast-forecasts")) |> 
    dplyr::filter(...1 == "parquet", ...2 == {theme}) |> 
    dplyr::select(model_id = ...3, reference_datetime = ...4, date = ...5) |>
    dplyr::mutate(model_id = gsub("model_id=", "", model_id),
           reference_datetime = 
             gsub("reference_datetime=", "", reference_datetime),
           date = gsub("date=", "", date)) |>
    dplyr::collect()
  
  if(!collapse)
    groups <- groups |>
    mutate(s3 = paste0("s3://neon4cast-forecasts/parquet/", theme,
                       "/model_id=", model_id,
                       "/reference_datetime=",reference_datetime,
                       "/date=", date, "/part-0.parquet",
                       "?endpoint_override=", endpoint),
           https = paste0("https://", endpoint,
                          "/neon4cast-forecasts/parquet/", theme,
                          "/model_id=", model_id,
                          "/reference_datetime=",reference_datetime,
                          "/date=", date, "/part-0.parquet"),
    )
  
  if(!collapse) return(groups)
  
  # otherwise collapse down by reference_datetime
  groups |>
    group_by(model_id, date) |> 
    dplyr::summarise(reference_datetime = 
                       paste(reference_datetime, collapse=", "),
              .groups = "drop")
  
}
  



get_target <- function(theme, s3) {
  options("readr.show_progress"=FALSE)
  key <- glue::glue("{theme}/{theme}-targets.csv.gz")
  read4cast::read_forecast(key, s3 = s3)
}


## NOTE cannot append to *remote* S3 sources, so prov_has / prov_add must work local and sync.
prov_has <- function(id, prov, colname="prov") {
  ## would be fast even with remote file
  prov |> dplyr::filter(.data[[colname]] == id) |> nrow() >= 1
}

prov_add <- function(id, local_prov = "scoring_provenance.csv") {
  new_prov <-  dplyr::tibble(prov=NA_integer_, new_id=id)
  ## Can never append to S3-hosted objects
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
  prov <- arrow::open_dataset(local_prov, format="csv",
                              schema = arrow::schema(prov = arrow::string(),
                                            new_id = arrow::string()))
  path <- s3_prov$path(local_prov)
  prov <- arrow::write_csv_arrow(prov, path)
}

## arrow method is a bit slower?
get_fcst_arrow <- function(endpoint, bucket, theme, group) {
  paste0("s3://", fs::path(bucket, "parquet/", theme),
         "/model_id=", group$model_id, "/reference_datetime=",
         stringi::stri_split_fixed(group$reference_datetime, ", ")[[1]],
         "/date=", group$date, "/part-0.parquet",
         "?endpoint_override=", endpoint) |> 
    arrow::open_dataset() |> 
    dplyr::mutate(file = add_filename(),
                  model_id = 
                    gsub(".*model_id=(\\w+).*", "\\1", file),
                  reference_datetime = 
                    gsub(".*reference_datetime=(\\d{4}-\\d{2}-\\d{2}).*",
                         "\\1", file),
                  date = 
                    gsub(".*date=(\\d{4}-\\d{2}-\\d{2}).*", "\\1", file)
    ) |>
    dplyr::filter(!is.na(family)) |>
    dplyr::collect()
}


globalVariables(c("...1", "...2", "...3", "...4", "...5", 
                  "add_filename", "theme", "n"), 
                package="score4cast")
