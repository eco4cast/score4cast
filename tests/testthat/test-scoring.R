

test_that("scoring", {

  fc <- "https://data.ecoforecast.org/forecasts/aquatics/aquatics-2020-09-01-EFInull.csv.gz"
  df <- score(fc, 
        "https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz")
  expect_true(inherits(df, "data.frame"))
})

test_that("ticks scores", {
  
  fc <- "https://data.ecoforecast.org/forecasts/ticks/ticks-2021-03-31-SynergisTicks.csv"
  target <- readr::read_csv("https://data.ecoforecast.org/targets/ticks/ticks-targets.csv.gz") %>%
    pivot_target(target_vars = TARGET_VARS) %>% 
    mutate(target_id = "ticks")
  forecast_df <- read4cast::read_forecast(fc)
  forecast <- forecast_df %>% 
    mutate(filename= basename(fc)) %>%
    pivot_forecast(target_vars = TARGET_VARS)
  crps_logs_score(forecast, target) %>% filter(!is.na(observed))
  
  
  df <- score(forecast_df, "https://data.ecoforecast.org/targets/ticks/ticks-targets.csv.gz")
  expect_true(inherits(df, "data.frame"))
  
})

test_that("scoring ncdf files", {
  skip_on_os("windows")
  skip_on_os("mac")
  
  # NCDF read fails on windows CI, not sure why
  nc_fc <- "https://data.ecoforecast.org/forecasts/terrestrial_30min/terrestrial_30min-2022-01-01-hist30min.nc"
  df <- score(nc_fc, "https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz", theme="terrestrial_30min")
  expect_true(inherits(df, "data.frame"))
})

test_that("mean scores", {
  fc <- "https://data.ecoforecast.org/forecasts/aquatics/aquatics-2021-05-01-wbears_rnn.csv"
  df <- score(fc, "https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz", theme="aquatics")
  expect_true(inherits(df, "data.frame"))
  null <- "https://data.ecoforecast.org/forecasts/aquatics/aquatics-2021-05-01-EFInull.csv.gz"
  dn <- score(null, "https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz", theme="aquatics")
  expect_true(inherits(dn, "data.frame"))
  df <- dplyr::bind_rows(df,dn)
  filled <- fill_scores(df)
  expect_gt(nrow(filled),0)
  
  leaderboard <- mean_scores(filled)
  expect_gt(nrow(leaderboard),0)
})

