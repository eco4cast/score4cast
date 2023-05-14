
theme = "aquatics"
local_prov =  paste0(theme, "-scoring-prov.csv")
# remotes::install_deps()
devtools::load_all()
library(arrow)
ignore_sigpipe()
readRenviron(path.expand("~/.Renviron"))
Sys.setenv("AWS_EC2_METADATA_DISABLED"="TRUE")
Sys.unsetenv("AWS_DEFAULT_REGION")


endpoint = "data.ecoforecast.org"
s3_forecasts <- arrow::s3_bucket("neon4cast-forecasts", endpoint_override = endpoint)
s3_targets <- arrow::s3_bucket("neon4cast-targets", endpoint_override = endpoint)
## Publishing Requires AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY set
s3_scores <- arrow::s3_bucket("neon4cast-scores", endpoint_override = endpoint)
s3_prov <- arrow::s3_bucket("neon4cast-prov", endpoint_override = endpoint, anonymous=TRUE)
s3_scores <- arrow::SubTreeFileSystem$create("~/test_scores", arrow::LocalFileSystem$create())
options("mc.cores"=4L)



prov_download(s3_prov, local_prov)
prov_df <- readr::read_csv(local_prov, show_col_types = FALSE)
s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}", theme=theme))
target <- get_target(theme, s3_targets)



## slow when lots of files, even with schema!  just listing files is really slow... parquet hates small partitions.... (benchmark csv????)
#bench::bench_time({ # 32 minutes!
#  fc <- arrow::open_dataset(fc_path, schema=forecast_schema())
#})

## use filepaths to determine groups of `model_id`, `date` , where date is from `datetime`
#bench::bench_time({ # 32 minutes!
#  grouping <- get_grouping(fc_path)
#})

s3_inv <- arrow::s3_bucket("neon4cast-inventory",  endpoint_override = "data.ecoforecast.org", anonymous=TRUE)
grouping <- get_grouping(s3_inv, theme)

n <- nrow(grouping)

pb <- progress::progress_bar$new(
  format = glue::glue("  scoring {theme} [:bar] :percent in :elapsed,",
                      " eta: :eta"),
  total = n, 
  clear = FALSE, width= 80)  

# this for loop is the same as:
# parallel::mclapply(1:n, score_group, grouping, fc, target, prov_df, local_prov, s3_scores_path, pb)
#for (i in 1:n) { 
parallel::mclapply(1:n, function(i) {  
  ## this is the score_group() function:
  pb$tick()
  group <- grouping[i,]
  ref <- lubridate::as_datetime(group$date)
  
  # NOTE: we cannot 'prefilter' grouping by prov, since once we have tg
  # we want to use it to score, not access it twice...
  tg <- target |>
    filter(datetime >= ref, datetime < ref+lubridate::days(1))
  
  ## ID changes only if target has changed between dates for this group
  id <- rlang::hash(list(group,  tg))

  if (!prov_has(id, prov_df)) {

    bucket <- s3_forecasts$base_path
    get_fcst_arrow(endpoint, bucket, theme, group) |> 
      filter(!is.na(family)) |>
      crps_logs_score(tg) |>
      mutate(date = group$date) |>
      arrow::write_dataset(s3_scores_path,
                           partitioning = c("model_id",
                                            "date"))
    prov_add(id, local_prov)
  }
})



## arrow method is a bit slower?
get_fcst_arrow <- function(endpoint, bucket, theme, group) {
  paste0("s3://", bucket, "parquet/", theme,
         "/model_id=", group$model_id, "/reference_datetime=",
         stringi::stri_split_fixed(group$reference_datetime, ", ")[[1]],
         "/date=", group$date, "/part-0.parquet",
         "?endpoint_override=", endpoint) |> 
    arrow::open_dataset(schema=forecast_schema()) |> 
    collect()
}



## Helper fuction to open a forecast subset with duckdb
get_fcst_duckdb <- function(conn, endpoint, bucket, theme, group) {
  
  conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  DBI::dbExecute(conn, "INSTALL 'httpfs';")
  DBI::dbExecute(conn, "LOAD 'httpfs';")
  
  
  parquet <- 
    paste0("[", paste0(paste0("'",
                              "https://", endpoint, "/", bucket, "parquet/", theme,
                              "/model_id=", group$model_id, "/reference_datetime=",
                              stringi::stri_split_fixed(group$reference_datetime, ", ")[[1]],
                              "/date=", group$date, "/part-0.parquet", "'"), collapse = ","),
           "]")
  
  tblname <- "forecast_subset"
  view_query <-
    glue::glue("CREATE VIEW '{tblname}' ", 
               "AS SELECT * FROM parquet_scan({parquet}, ",
               "HIVE_PARTITIONING=true);")
  DBI::dbSendQuery(conn, view_query)
  fc_i <- dplyr::tbl(conn, tblname) |> dplyr::collect()
  DBI::dbSendQuery(conn, glue::glue("DROP VIEW {tblname}"))
  fc_i
}



