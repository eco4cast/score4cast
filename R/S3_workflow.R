

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
#' @param endpoint domain name for forecast/target download URL
#' @export
score_theme <- function(theme, 
                        s3_forecasts, 
                        s3_targets, 
                        s3_scores, 
                        s3_prov, 
                        endpoint){
  
  options("readr.show_progress"=FALSE)
  target <- get_target(theme, endpoint)
  forecast_urls <- get_forecasts(s3_forecasts, theme, endpoint)
  
  tictoc <- bench::bench_time({
    errors <- forecast_urls %>% 
      purrr::map(score_safely, 
                 target, 
                 s3_prov, 
                 s3_scores)
  })
  
  ## warn about errors (e.g. curl upload failures)
  warnings <- purrr::compact(purrr::map(errors, ~ .x$error$message))
  purrr::map(warnings, warning)
  ## message and timing
  options("readr.show_progress"=NULL)
  message(paste("scored", theme, "in", tictoc[[2]]))
  
}


get_target <- function(theme, endpoint) {
  
  ## alternately: could use already-pivoted monthly files
  #path <- s3_targets$path(glue::glue("{theme}/monthly", theme=theme))
  #target <- arrow::open_dataset(path, format="csv", 
  #                              skip_rows = 1, schema = target_schema) 
  
  path <- glue::glue("https://{endpoint}/targets/{theme}/{theme}-targets.csv.gz",
                     theme=theme, endpoint = endpoint)
  target <- 
    readr::read_csv(path, show_col_types = FALSE) %>% 
    pivot_target(TARGET_VARS) %>% 
    mutate(theme = theme)
  
}

get_forecasts <- function(s3_forecasts, theme, endpoint) {
  ## extract URLs for forecasts & targets
  forecasts <- c(stringr::str_subset(s3_forecasts$ls(theme), "[.]csv(.gz)?"),
                 stringr::str_subset(s3_forecasts$ls(theme), "[.]nc"))
  ## Weird to require endpoint and construct raw URLs
  forecast_urls <- paste0("https://", endpoint, "/forecasts/", forecasts )
  forecast_urls
}

# A relatively generic scoring function which
# takes a pivoted targets but un-pivoted forecast
# if pivot_* fns were smart they could conditionally pivot
score_it <- function(forecast_df, target_df) {
  forecast_df %>%
    pivot_forecast(TARGET_VARS) %>%
    crps_logs_score(target_df) %>%
    include_horizon()
}

# Read a forecast file + target file and score them conditionally on prov
score_if <- function(forecast_file, 
                     target, 
                     s3_prov,
                     s3_scores,
                     score_file = score_dest(forecast_file, 
                                             s3_scores,
                                             "parquet")
) {
  forecast_df <- 
    read4cast::read_forecast(forecast_file) %>% 
    mutate(filename = forecast_file)
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
  theme <- strsplit(out, "-")[[1]][[1]]
  year <-  strsplit(out, "-")[[1]][[2]]
  path <- paste(type, theme, year, paste0(out, ".", type), sep="/")
  
  s3_scores$path(path)
}

## Facilitate reading in targets
target_schema <- arrow::schema(
  site       = arrow::string(),
  x          = arrow::float64(),
  y          = arrow::float64(),
  z          = arrow::float64(),
  time       = arrow::timestamp("us", "UTC"),
  target     = arrow::string(), # should become "variable"
  observed   = arrow::float64(),
  theme      = arrow::string()
  # year = arrow::int32()     ## can't use yet, see: https://issues.apache.org/jira/browse/ARROW-15879?filter=-2
)


## Should become optional to pivot_forecast()
TARGET_VARS <- c("oxygen", 
                 "temperature", "richness", "abundance", "nee", "le", "vswc", 
                 "gcc_90", "rcc_90", "ixodes_scapularis", "amblyomma_americanum")



