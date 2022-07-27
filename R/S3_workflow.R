

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
#' @export
score_theme <- function(theme, 
                        s3_forecasts, 
                        s3_targets, 
                        s3_scores, 
                        s3_prov, 
                        after = as.Date("2022-01-01")){
  

  
  options("readr.show_progress"=FALSE)
  target <- get_target(theme, s3_targets)
  forecasts <- s3_forecasts$ls(theme)
  
  if(!is.null(after)){
    fcs <- basename(forecasts)
    dates <- stringr::str_extract(fcs, "\\d{4}-\\d{2}-\\d{2}")
    dates <- as.Date(dates)
    forecasts <- forecasts[dates >= after]
  }
  
  pb <- progress::progress_bar$new(
    format = glue::glue("  scoring {theme} [:bar] :percent in :elapsed,",
                        " eta: :eta"),
    total = length(forecasts), 
    clear = FALSE, width= 80)

  errors <- forecasts %>% 
      purrr::map(function(x) {
        pb$tick()
        score_safely(x, 
                     target = target, 
                     s3_prov = s3_prov, 
                     s3_scores = s3_scores, 
                     s3_forecasts = s3_forecasts)
      })
  
    
  ## warn about errors (e.g. curl upload failures)
  warnings <- unique(purrr::compact(purrr::map(errors, ~ .x$error$message)))
  purrr::map(warnings, warning, call.=FALSE)
  ## message and timing
  options("readr.show_progress"=NULL)
  unscored <- purrr::map_lgl(purrr::map(errors, "result"),is.null)
  error <- purrr::map(errors[unscored], "error")
  invisible(list(urls = forecast_urls[unscored], error = error))
}

## Optional once forecasts and targets files use long variable format
TARGET_VARS <- c("oxygen", 
                 "temperature", "chla", "richness", "abundance", "nee", "le", "vswc", 
                 "gcc_90", "rcc_90", "ixodes_scapularis", "amblyomma_americanum",
                 "Amblyomma americanum")


get_target <- function(theme, s3) {
  key <- glue::glue("{theme}/{theme}-targets.csv.gz")
  read4cast::read_forecast(key, s3 = s3_targets) %>%
    mutate(target_id = theme) %>%
    pivot_target(TARGET_VARS)
}


# A relatively generic scoring function which
# takes a pivoted targets but un-pivoted forecast
# if pivot_* fns were smart they could conditionally pivot
score_it <- function(forecast_df, target_df) {
  
  suppressMessages({ # don't show "joining by" msg
  forecast_df %>%
    pivot_forecast(TARGET_VARS) %>%
    crps_logs_score(target_df) %>%
    include_horizon()
  })
}

# Read a forecast file + target file and score them conditionally on prov
score_if <- function(forecast_file, 
                     target, 
                     s3_prov,
                     s3_scores,
                     s3_forecasts,
                     score_file = score_dest(forecast_file, 
                                             s3_scores,
                                             "parquet")
) {
  
  suppressMessages({ ## no message about 'new columns'
    forecast_df <- 
      read4cast::read_forecast(forecast_file, s3 = s3_forecasts) %>% 
      mutate(filename = basename(forecast_file))
  })
  target_df <- subset_target(forecast_df, target)
  id <- rlang::hash(list(forecast_df, target_df))
  # score only unique combinations of subset of targets + forecast
  if (!prov_has(id, s3_prov)) {
    score_it(forecast_df, target_df) %>%
      arrow::write_parquet(score_file)
  }
  prov_add(id, s3_prov)
  invisible(id)
}

## we care if there are any errors
score_safely <- purrr::safely(score_if)


# "target" can be a pointer to S3 bucket
# works with local or target data.frame too
subset_target <- function(forecast_df, target) {
  range <- forecast_df %>% 
    summarise(start = min(time),
              end=max(time))
  start <- lubridate::as_datetime(range$start[[1]])
  end <- lubridate::as_datetime(range$end[[1]])
  year <- lubridate::year(start)
  target %>%
    filter(
      #year >= {{year}}, # potential speed up, but arrow bug...
      time >= {{start}}, 
      time <= {{end}}) %>%
    dplyr::collect()
}

## Lots of alternative ways to write these; could use local file but would have to sync
## Note that S3 cannot 'append' to a stream
## A poor man's index: says only if id has been seen before
prov_has <- function(id, s3_prov) {
  prov <-  s3_prov$ls()
  any(grepl(id, prov))
}
prov_add <- function(id, s3_prov) {
  x <- s3_prov$OpenOutputStream(id)
  x$write(raw()) # no actual information
  x$close()
}
## Note, we can still access timestamp on prov, and purge older than etc

score_dest <- function(forecast_file, s3_scores, type="parquet"){ 
  out <- tools::file_path_sans_ext(basename(forecast_file), compression = TRUE)
  target_id <- strsplit(out, "-")[[1]][[1]]
  year <-  strsplit(out, "-")[[1]][[2]]
  path <- paste(type, target_id, year, paste0(out, ".", type), sep="/")
  
  s3_scores$path(path)
}





