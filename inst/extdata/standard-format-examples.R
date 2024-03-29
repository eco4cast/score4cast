library(tibble)
library(dplyr)
library(lubridate)
# "sample" = "ensemble"
ensemble_forecast <- tibble::tribble(
  ~model_id, ~reference_datetime,    ~site_id, ~datetime,                  ~family,    ~parameter, ~variable,     ~prediction,
  "ensemble_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00Z",  "sample",  "1",       "oxygen",      5.55,
  "ensemble_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00Z",  "sample",  "1",       "temperature", 24.5,
  "ensemble_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00Z",  "sample",  "2",       "oxygen",      5.25,
  "ensemble_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00Z",  "sample",  "2",       "temperature", 26.5,
) |>
  dplyr::mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
                datetime = lubridate::as_datetime(datetime))


# normal = normalal = gaussian 
# larger example just as sanity check that we handle additional grouping variables (site_id, datetime) correctly:
gaussian_forecast <- tibble::tribble(
  ~model_id, ~reference_datetime,    ~site_id, ~datetime,                  ~family,    ~parameter, ~variable,     ~prediction,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00Z",  "normal",  "mu",       "oxygen",        5.55,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00Z",  "normal",  "sigma",    "oxygen",        0.2,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00Z",  "normal",  "mu",       "temperature",   24.5,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-02 00:00:00Z",  "normal",  "sigma",    "temperature",   0.5,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-03 00:00:00Z",  "normal",  "mu",       "oxygen",        3,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-03 00:00:00Z",  "normal",  "sigma",    "oxygen",        0.1,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-03 00:00:00Z",  "normal",  "mu",       "temperature",   20,
  "gauss_team", "2022-02-01", "BARC",   "2022-02-03 00:00:00Z",  "normal",  "sigma",    "temperature",   0.1,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-02 00:00:00Z",  "normal",  "mu",       "oxygen",        1,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-02 00:00:00Z",  "normal",  "sigma",    "oxygen",        0.2,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-02 00:00:00Z",  "normal",  "mu",       "temperature",   31,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-02 00:00:00Z",  "normal",  "sigma",    "temperature",   0.5,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-03 00:00:00Z",  "normal",  "mu",       "oxygen",        2,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-03 00:00:00Z",  "normal",  "sigma",    "oxygen",        0.1,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-03 00:00:00Z",  "normal",  "mu",       "temperature",   32,
  "gauss_team", "2022-02-01", "ORNL",   "2022-02-03 00:00:00Z",  "normal",  "sigma",    "temperature",   0.1,)  |>
  dplyr::mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
                datetime = lubridate::as_datetime(datetime))


small_gauss <- gaussian_forecast |> dplyr::filter(site_id == "BARC", datetime ==  "2022-02-02 00:00:00")

ex_target <- tibble::tribble(
  ~reference_datetime,    ~site_id, ~datetime,                  ~variable,     ~observed,
  "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "oxygen",      5.00,
  "2022-02-01", "BARC",   "2022-02-02 00:00:00",  "temperature", 24.00,
  "2022-02-01", "BARC",   "2022-02-03 00:00:00",  "oxygen",      1.00,
  "2022-02-01", "BARC",   "2022-02-03 00:00:00",  "temperature", 15.00,
  "2022-02-01", "ORNL",   "2022-02-02 00:00:00",  "oxygen",      5.00,
  "2022-02-01", "ORNL",   "2022-02-02 00:00:00",  "temperature", 24.00,
  "2022-02-01", "ORNL",   "2022-02-03 00:00:00",  "oxygen",      1.00,
  "2022-02-01", "ORNL",   "2022-02-03 00:00:00",  "temperature", 15.00,  ) |>
  dplyr::mutate(reference_datetime = lubridate::as_datetime(reference_datetime),
                datetime = lubridate::as_datetime(datetime))

## could use any of the above forecasts, can even stack forecasts:
ex_forecast <- dplyr::bind_rows(gaussian_forecast, ensemble_forecast)


