# Consider turning to fable first, and working on a dist column
efi_to_fable <- function(df) {
  key <- c("model_id", "reference_datetime", "site_id", "variable") 
  tb <- df |> 
    group_by(model_id, reference_datetime, site_id, datetime, variable, family) |> 
    summarise(prediction = infer_dist(family, parameter, prediction),
              .groups = "drop")
  
  fc <- tb  |> 
    fabletools::as_fable(response = "prediction", 
                         distribution = prediction, 
                         index = datetime,
                         key = dplyr::any_of(key))               
  
  fc
}


## then we can use distributional:: functions
infer_dist <- function(family, parameter, prediction) {
  names(prediction) = parameter
  
  ## operates on a unique observation (model_id, reference_datetime, site_id, datetime, family, variable)
  fam <- unique(family)
  arg <- switch(fam, 
                sample = list(list(prediction)),
                as.list(prediction)
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
# fc |> mutate(mean = mean(prediction), sd = sqrt(distributional::variance(prediction)))



# obs <- fc |> as_tsibble() |> select(-prediction) |> rename(prediction = observation)
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
         bernoulli = scoringRules::logs_binom(.actual,  size = 1, prob = par$prob),
         beta = scoringRules::logs_beta(y, shape1 = par$shape1, shape1 = par$shape2),
         uniform = scoringRules::logs_unif(y, min = par$min, max = par$max),
         gamma = scoringRules::logs_gamma(y, shape = par$shape, rate = par$rate),
         logistic = scoringRules::logs_logis(y, location = par$location, scale = par$scale),
         exponential = scoringRules::logs_exp(y, rate = par$rate),
         poisson = scoringRules::logs_pois(y, lambda = par$lambda),
         sample = scoringRules::logs_sample(.actual, dat = unlist(par$x))
         
         
         
         
  )
}

# This works
# obs <- fc |> as_tsibble() |> select(-prediction) |> rename(prediction = observation)
# fc |> accuracy(obs)




