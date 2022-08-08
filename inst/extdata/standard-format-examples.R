library(tibble)

# "sample" = "ensemble"
ensemble_forecast <- tibble::tribble(
  ~model_id, ~pub_time,    ~site_id, ~time,                  ~family,    ~parameter, ~variable,     ~predicted,
  "ensemble_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "sample",  "1",       "oxygen",      5.55,
  "ensemble_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "sample",  "1",       "temperature", 24.5,
  "ensemble_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "sample",  "2",       "oxygen",      5.25,
  "ensemble_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "sample",  "2",       "temperature", 26.5,
)

# norm = normal = gaussian 
# larger example just as sanity check that we handle additional grouping variables (site_id, time) correctly:
gaussian_forecast <- tibble::tribble(
  ~model_id, ~pub_time,    ~site_id, ~time,                  ~family,    ~parameter, ~variable,     ~predicted,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "norm",  "mean",   "oxygen",        5.55,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "norm",  "sd",     "oxygen",        0.2,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "norm",  "mean",   "temperature",   24.5,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "norm",  "sd",     "temperature",   0.5,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-03 00:00:00",  "norm",  "mean",   "oxygen",        3,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-03 00:00:00",  "norm",  "sd",     "oxygen",        0.1,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-03 00:00:00",  "norm",  "mean",   "temperature",   20,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-03 00:00:00",  "norm",  "sd",     "temperature",   0.1,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-02 00:00:00",  "norm",  "mean",   "oxygen",        1,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-02 00:00:00",  "norm",  "sd",     "oxygen",        0.2,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-02 00:00:00",  "norm",  "mean",   "temperature",   31,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-02 00:00:00",  "norm",  "sd",     "temperature",   0.5,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-03 00:00:00",  "norm",  "mean",   "oxygen",        2,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-03 00:00:00",  "norm",  "sd",     "oxygen",        0.1,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-03 00:00:00",  "norm",  "mean",   "temperature",   32,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-03 00:00:00",  "norm",  "sd",     "temperature",   0.1,)  

small_gauss <- gaussian_forecast |> filter(site_id == "BARC", time ==  "2022-02-02 00:00:00")

target <- tibble::tribble(
  ~pub_time,    ~site_id, ~time,                  ~variable,     ~observed,
  "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "oxygen",      5.00,
  "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "temperature", 24.00,
  "2022-02-01", "BARC",   "2022-02-03 00:00:00",  "oxygen",      1.00,
  "2022-02-01", "BARC",   "2022-02-03 00:00:00",  "temperature", 15.00,
  "2022-02-01", "ORNL",   "2022-02-02 00:00:00",  "oxygen",      5.00,
  "2022-02-01", "ORNL",   "2022-02-02 00:00:00",  "temperature", 24.00,
  "2022-02-01", "ORNL",   "2022-02-03 00:00:00",  "oxygen",      1.00,
  "2022-02-01", "ORNL",   "2022-02-03 00:00:00",  "temperature", 15.00,  )


## could use any of the above forecasts, can even stack forecasts:
forecast <- bind_rows(small_gauss, ensemble_forecast)


