# Consider turning to fable first, and working on a dist column
efi_to_fable <- function(df) {
  key <- c("model_id", "start_time", "site_id", "variable") 
  tb <- df |> 
    group_by(model_id, start_time, site_id, time, variable, family, observed) |> 
    summarise(predicted = infer_dist(family, parameter, predicted),
              .groups = "drop")
  
  fc <- tb  |> 
    mutate(time = lubridate::as_date(time)) |> # Ick tsibble bug
    fabletools::as_fable(response = "predicted", 
                         distribution = predicted, 
                         index = time,
                         key = dplyr::any_of(key))               
  
  fc
}

## score using fable: 
# source(system.file("extdata/standard-format-examples.R", package="score4cast"))
# fc <- inner_join(ex_forecast, ex_target) |>  efi_to_fable()
# obs <- fc |> as_tsibble() |> select(-predicted) |> rename(predicted = observed)
# fc |> accuracy(obs, measures = lst(log_score), by = c("model_id", "site_id", "start_time", "time", "variable"))


# fc |> accuracy(obs)




log_score <- function(.dist, .actual, ...) {
  par <- distributional::parameters(.dist)
  fam <- unique(stats::family(.dist))
  if(length(fam) > 1) {
    stop(paste("models with different distribution families must be assessed seperately."))
  }
  
  switch(fam,
         normal = scoringRules::logs_norm(.actual, mean = par$mu, sd = par$sigma),
         lognormal = scoringRules::logs_lnorm(.actual, mean = par$mu, sd = par$sigma),
         sample = scoringRules::logs_sample(.actual, dat = unlist(par$x))
  )
}

# This works
# obs <- fc |> as_tsibble() |> select(-predicted) |> rename(predicted = observed)
# fc |> accuracy(obs)




