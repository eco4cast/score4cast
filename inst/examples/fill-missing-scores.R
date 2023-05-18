devtools::load_all()
library(arrow)
library(dplyr)
ignore_sigpipe()

# efi_update_inventory()

theme <- "aquatics"
endpoint <- "data.ecoforecast.org"
s3_inv <- arrow::s3_bucket("neon4cast-inventory", 
                           endpoint_override = endpoint, anonymous=TRUE)



## Detect cases where we have model_id / reference_datetime / date in forecast but not in scores
fcst_groups <- arrow::open_dataset(s3_inv$path("neon4cast-forecasts")) |> 
  dplyr::filter(...1 == "parquet", ...2 == {theme}) |> 
  dplyr::select(model_id = ...3, reference_datetime = ...4, date = ...5) |>
  dplyr::mutate(model_id = gsub("model_id=", "", model_id),
                reference_datetime = 
                  gsub("reference_datetime=", "", reference_datetime),
                date = gsub("date=", "", date)) |>
  dplyr::collect()


s3_scores <- arrow::s3_bucket("neon4cast-scores", endpoint_override = endpoint)


s3_ds <- arrow::open_dataset(s3_scores)
scored_groups <- s3_ds |>
  dplyr::select(model_id, reference_datetime, date) |> 
  dplyr::distinct()  |> dplyr::collect() 

grouping <- anti_join(fcst_groups, scored_groups) |>
  group_by(model_id, date) 


grouping <- grouping |> mutate(s3 = paste0("s3://neon4cast-forecasts/parquet/", theme,
                               "/model_id=", model_id,
                               "/reference_datetime=",reference_datetime,
                               "/date=", date, "/part-0.parquet",
                               "?endpoint_override=", endpoint))

#grouping_standard <- grouping |> 
#  dplyr::summarise(reference_datetime = 
#                     paste(reference_datetime, collapse=", "),
#                   .groups = "drop")


fc |> 
  filter(!is.na(family)) |> #hhhmmmm? what should we be doing about these forecasts?
  crps_logs_score(tg) |>
  mutate(date = as.Date(datetime)) |>
  arrow::write_dataset(s3_scores_path,
                       partitioning = c("model_id",
                                        "date"))


endpoint = "data.ecoforecast.org"
s3_forecasts <- arrow::s3_bucket("neon4cast-forecasts", endpoint_override = endpoint)
s3_targets <- arrow::s3_bucket("neon4cast-targets", endpoint_override = endpoint)
s3_prov <- arrow::s3_bucket("neon4cast-prov", endpoint_override = endpoint)
local_prov =  paste0(theme, "-scoring-prov.csv")

prov_download(s3_prov, local_prov)
prov_df <- readr::read_csv(local_prov, col_types = "cc")
s3_scores_path <- s3_scores$path(glue::glue("parquet/{theme}", theme=theme))
target <- get_target(theme, s3_targets)


pb <- progress::progress_bar$new(
  format = glue::glue("  scoring {theme} [:bar] :percent in :elapsed,",
                      " eta: :eta"),
  total = nrow(grouping), 
  clear = FALSE, width= 80)  


  for (i in seq_along(grouping[[1]])) { 
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

prov_upload(s3_prov, local_prov)







