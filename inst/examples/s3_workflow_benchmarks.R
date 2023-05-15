devtools::load_all()
library(arrow)
ignore_sigpipe()
readRenviron(path.expand("~/.Renviron"))
Sys.setenv("AWS_EC2_METADATA_DISABLED"="TRUE")
Sys.unsetenv("AWS_DEFAULT_REGION")

endpoint = "data.ecoforecast.org"
s3_forecasts <- arrow::s3_bucket("neon4cast-forecasts", endpoint_override = endpoint)
s3_targets <- arrow::s3_bucket("neon4cast-targets", endpoint_override = endpoint)
s3_inv <- arrow::s3_bucket("neon4cast-inventory",  endpoint_override = "data.ecoforecast.org")
## Publishing Requires AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY set
s3_scores <- arrow::s3_bucket("neon4cast-scores", endpoint_override = endpoint)
s3_prov <- arrow::s3_bucket("neon4cast-prov", endpoint_override = endpoint)


theme = "aquatics"
local_prov =  paste0(theme, "-scoring-prov.csv")

prov_download(s3_prov, local_prov)
prov_df <- readr::read_csv(local_prov, show_col_types = FALSE)
s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}", theme=theme))
target <- get_target(theme, s3_targets)

grouping <- get_grouping(s3_inv, theme, collapse = TRUE)

## benchmark on a subset:
grouping <- grouping |> filter(model_id == "cb_prophet")
n <- nrow(grouping)

pb <- progress::progress_bar$new(
  format = glue::glue("  scoring {theme} [:bar] :percent in :elapsed,",
                      " eta: :eta"),
  total = n, 
  clear = FALSE, width= 80)  

# this for loop is the same as:
# parallel::mclapply(1:n, score_group, grouping, fc, target, prov_df, local_prov, s3_scores_path, pb)

bench::bench_time({
  for (i in 1:n) { 
  #furrr::future_map(1:n, function(i) {  
    ## this is the score_group() function:
    pb$tick()
    group <- grouping[i,]
    ref <- lubridate::as_datetime(group$date)
    
    # NOTE: we cannot 'prefilter' grouping by prov, since once we have tg
    # we want to use it to score, not access it twice...
    tg <- target |>
      filter(datetime >= ref, datetime < ref+lubridate::days(1))
    
    ## ID changes only if target has changed between dates for this group
    id <- rlang::hash(list(grouping[i, c("model_id", "date")],  tg))
    new_id <- rlang::hash(list(group,  tg))
    
    if (! (prov_has(id, prov_df, "prov") ||
           prov_has(new_id, prov_df, "new_id"))
       ) {
        message(paste(i, group[i,1:2], "\n"))
        fc <- get_fcst_arrow(endpoint, "neon4cast-forecasts", theme, group)
        fc |> 
        filter(!is.na(family)) |> #hhhmmmm? what should we be doing about these forecasts?
        crps_logs_score(tg) |>
        mutate(date = as.Date(datetime)) |>
        arrow::write_dataset(s3_scores_path,
                             partitioning = c("model_id",
                                              "date"))
      prov_add(new_id, local_prov)
    }
  }
})


## arrow method is a bit slow

