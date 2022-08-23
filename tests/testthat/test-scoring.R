
fc <- "https://data.ecoforecast.org/neon4cast-forecasts/beetles/beetles-2021-05-01-EFInull.csv.gz"

test_that("family-based standards work", {
  
  source(system.file("extdata/standard-format-examples.R",
                     package="score4cast",
                     mustWork = TRUE))
  
  scores <- crps_logs_score(ex_forecast, ex_target)
  expect_true(inherits(scores, "data.frame"))
  
  who <- colnames(scores)
  expect_true(all(c("site_id", "time", "family",
                    "variable", "observed", "crps",
                    "logs", "mean", "sd", "quantile10", 
                    "start_time", "model_id") %in%
    who))
})


test_that("unit tests", {

  
  source(system.file("extdata/standard-format-examples.R",
                     package="score4cast",
                     mustWork = TRUE))
  
  scores <- score(ex_forecast, ex_target)
  
  expect_true(inherits(scores, "data.frame"))
  
  who <- colnames(scores)
  expect_true(all(c("site_id", "time", "family",
                    "variable", "observed", "crps",
                    "logs", "mean", "sd", "quantile10", 
                    "start_time", "model_id") %in%
                    who))
  
  scores <- crps_logs_score(ex_forecast, ex_target) |> include_horizon()
  expect_true(inherits(scores, "data.frame"))
  
  who <- colnames(scores)
  expect_true(all(c("site_id", "time", "family",
                    "variable", "observed", "crps",
                    "logs", "mean", "sd", "quantile10", 
                    "start_time", "model_id", "horizon") %in%
                    who))
  
})
  
test_that("unit tests", {
    
  
  
  source(system.file("extdata/standard-format-examples.R",
                     package="score4cast",
                     mustWork = TRUE))
  
  scores <- score(ex_forecast, ex_target)
  filled <- fill_scores(scores, "gauss_team")
  expect_true(inherits(filled, "data.frame"))
  expect_gt(nrow(filled), 1)
  
  df <- mean_scores(filled)
  expect_true(inherits(df, "data.frame"))
  expect_gt(nrow(df), 1)
  
  })




## All these tests involve backwards-compabible formats 






