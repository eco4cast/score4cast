library(duckdb)
library(glue)


get_fcst <- function(conn, endpoint, bucket, theme, group) {
  
  conn <- DBI::dbConnect(duckdb(), ":memory:")
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
  view_query <-glue::glue("CREATE VIEW '{tblname}' ", 
                    "AS SELECT * FROM parquet_scan({parquet}, HIVE_PARTITIONING=true);")
  DBI::dbSendQuery(conn, view_query)
  fc_i <- dplyr::tbl(conn, tblname) |> dplyr::collect()
  DBI::dbSendQuery(conn, glue::glue("DROP VIEW {tblname}"))
  fc_i
}


bench::bench_time( # 5.46 sec
  fc <- get_fcst(conn, endpoint, bucket, theme, group)
)

bench::bench_time({ # 14.6 seconds
fc_i <- paste0("s3://", bucket, "parquet/", theme,
               "/model_id=", group$model_id, "/reference_datetime=",
               stringi::stri_split_fixed(group$reference_datetime, ", ")[[1]],
               "/date=", group$date, "/part-0.parquet",
               "?endpoint_override=", endpoint) |> 
  arrow::open_dataset(schema=forecast_schema()) |> 
  collect()
})




## S3 can use a wildcard but it is much slower!
parquet_s3 <-  paste0("'", "s3://", bucket, "parquet/", theme,
           "/model_id=", group$model_id, "/reference_datetime=",
           "*",
           "/date=", group$date, "/part-0.parquet", "'")
endpoint <- "data.ecoforecast.org"
DBI::dbExecute(conn, glue("SET s3_endpoint='{endpoint}';"))
DBI::dbExecute(conn, glue("SET s3_url_style='path';"))
tblname <- "forecast_subset"
view_query <-glue::glue("CREATE VIEW '{tblname}' ", 
                        "AS SELECT * FROM parquet_scan({parquet_s3});")
DBI::dbSendQuery(conn, view_query)
fc_i <- tbl(conn, tblname) |> collect()
DBI::dbSendQuery(conn, glue::glue("DROP VIEW {tblname}"))



