

test_that("scoring", {

  fc <- "https://data.ecoforecast.org/forecasts/aquatics/aquatics-2020-09-01-EFInull.csv.gz"
  df <- score(fc, 
        "https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz")
  expect_true(inherits(df, "data.frame"))
  
  
})

test_that("scoring ncdf files", {
  skip_on_os("windows")
  # NCDF read fails on windows CI, not sure why
  nc_fc <- "https://data.ecoforecast.org/forecasts/terrestrial_30min/terrestrial_30min-2022-01-01-hist30min.nc"
  df <- score(nc_fc, "https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz", theme="terrestrial_30min")
  expect_true(inherits(df, "data.frame"))
})

test_that("mean scores", {
  # NCDF read fails on windows CI, not sure why
  nc_fc <- "https://data.ecoforecast.org/forecasts/aquatics/"
  df <- score(nc_fc, "https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz", theme="terrestrial_30min")
  expect_true(inherits(df, "data.frame"))
})

