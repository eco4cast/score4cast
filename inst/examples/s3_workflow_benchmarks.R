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


theme = "terrestrial_30min"
local_prov =  paste0(theme, "-scoring-prov.csv")

prov_download(s3_prov, local_prov)
prov_df <- readr::read_csv(local_prov, col_types = "cc")
s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}", theme=theme))
target <- get_target(theme, s3_targets)

grouping <- get_grouping(s3_inv, theme, collapse = TRUE)

## benchmark on a subset:
n <- nrow(grouping)

pb <- progress::progress_bar$new(
  format = glue::glue("  scoring {theme} [:bar] :percent in :elapsed,",
                      " eta: :eta"),
  total = n, 
  clear = FALSE, width= 80)  

# this for loop is the same as:
# parallel::mclapply(1:n, score_group, grouping, fc, target, prov_df, local_prov, s3_scores_path, pb)
conn <- DBI::dbConnect(duckdb::duckdb(), ":memory:")

group_indices <- seq_along(grouping[[1]])
group_indices <- group_indices[group_indices >=i]

pb <- progress::progress_bar$new(
  format = glue::glue("  scoring {theme} [:bar] :percent in :elapsed,",
                      " eta: :eta"),
  total = length(group_indices), 
  clear = FALSE, width= 80)

  #for (i in group_indices) { 
library(future)
plan(multisession, workers = 12)

#  furrr::future_map(1:n, function(i) {  
for(i in group_indices) {
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
        # message(paste(i, group[[1]], group[[2]], "\n"))
        fc <- get_fcst_duckdb_s3(conn, endpoint, "neon4cast-forecasts", theme, group)
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
#, .progress = TRUE)


get_fcst_duckdb <- function(conn, endpoint, bucket, theme, group) {
  
  DBI::dbExecute(conn, "INSTALL 'httpfs';")
  DBI::dbExecute(conn, "LOAD 'httpfs';")
  
  parquet <- 
    paste0("[", paste0(paste0("'",
                              "https://", endpoint, "/", bucket, "/parquet/", theme,
                              "/model_id=", group$model_id, "/reference_datetime=",
                              stringi::stri_split_fixed(group$reference_datetime, ", ")[[1]],
                              "/date=", group$date, "/part-0.parquet", "'"), collapse = ","),
           "]")
  
  tblname <- "forecast_subset"
  view_query <-glue::glue("CREATE VIEW '{tblname}' ", 
                          "AS SELECT * FROM parquet_scan({parquet}, HIVE_PARTITIONING=true);")
  DBI::dbSendQuery(conn, view_query)
  fc_i <- dplyr::tbl(conn, tblname) |> dplyr::collect()
  DBI::dbSendQuery(conn, glue::glue("DROP VIEW {tblname}"))
  fc_i
}



get_fcst_duckdb_s3 <- function(conn, endpoint, bucket, theme, group) {
  
  DBI::dbExecute(conn, "INSTALL 'httpfs';")
  DBI::dbExecute(conn, "LOAD 'httpfs';")
  DBI::dbExecute(conn, glue::glue("SET s3_endpoint='{endpoint}';"))
  DBI::dbExecute(conn, glue::glue("SET s3_url_style='path';"))
  parquet <- 
    paste0("[", paste0(paste0("'",
                              "s3://", bucket, "/parquet/", theme,
                              "/model_id=", group$model_id, "/reference_datetime=",
                              stringi::stri_split_fixed(group$reference_datetime, ", ")[[1]],
                              "/date=", group$date, "/part-0.parquet", "'"), collapse = ","),
           "]")
  
  tblname <- "forecast_subset"
  view_query <-glue::glue("CREATE VIEW '{tblname}' ", 
                          "AS SELECT * FROM parquet_scan({parquet}, HIVE_PARTITIONING=true);")
  DBI::dbSendQuery(conn, view_query)
  fc_i <- dplyr::tbl(conn, tblname) |> dplyr::collect()
  DBI::dbSendQuery(conn, glue::glue("DROP VIEW {tblname}"))
  fc_i
}


