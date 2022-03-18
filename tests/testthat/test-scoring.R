
test_that("scoring", {
  
  nc_fc <- "https://data.ecoforecast.org/forecasts/terrestrial_30min/terrestrial_30min-2022-01-01-hist30min.nc"
  score(nc_fc, "https://data.ecoforecast.org/targets/aquatics/aquatics-targets.csv.gz", theme="terrestrial_30min")
  
})