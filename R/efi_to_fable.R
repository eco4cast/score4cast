# Consider turning to fable first, and working on a dist column
efi_to_fable <- function(df) {
  key <- c("model_id", "reference_datetime", "site_id", "variable") 
  tb <- df |> 
    group_by(model_id, reference_datetime, site_id, datetime, variable, family) |> 
    summarise(predicted = infer_dist(family, parameter, predicted),
              .groups = "drop")
  
  fc <- tb  |> 
    fabletools::as_fable(response = "predicted", 
                         distribution = predicted, 
                         index = datetime,
                         key = dplyr::any_of(key))               
  
  fc
}


## then we can use distributional:: functions
infer_dist <- function(family, parameter, predicted) {
  names(predicted) = parameter
  
  ## operates on a unique observation (model_id, reference_datetime, site_id, datetime, family, variable)
  fam <- unique(family)
  arg <- switch(fam, 
                sample = list(list(predicted)),
                as.list(predicted)
  )
  fn <- eval(rlang::parse_expr(paste0("distributional::dist_", fam)))
  dist <- tryCatch(
    do.call(fn, arg),
    error = function(e) distributional::dist_missing(length(arg)),
    finally = distributional::dist_missing(length(arg)))
  dist
}

globalVariables(c("model_id", "reference_datetime", 
                  "site_id", "variable", "datetime"),
                package="score4cast")

## score using fable: 
# source(system.file("extdata/standard-format-examples.R", package="score4cast"))
# fc <- inner_join(ex_forecast, ex_target) |>  efi_to_fable()
# summary stats
# fc |> mutate(mean = mean(predicted), sd = sqrt(distributional::variance(predicted)))



# obs <- fc |> as_tsibble() |> select(-predicted) |> rename(predicted = observed)
# fc |> accuracy(obs, measures = lst(CRPS, log_score), by = c("model_id", "site_id", "reference_datetime", "datetime", "variable"))
# 
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




