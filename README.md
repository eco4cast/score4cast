
<!-- README.md is generated from README.Rmd. Please edit that file -->

# score4cast

<!-- badges: start -->

[![R-CMD-check](https://github.com/eco4cast/score4cast/workflows/R-CMD-check/badge.svg)](https://github.com/eco4cast/score4cast/actions)
[![CRAN
status](https://www.r-pkg.org/badges/version/score4cast)](https://CRAN.R-project.org/package=score4cast)
<!-- badges: end -->

The goal of score4cast is to provide a convenient interface to score
ecological forecasts that conform the EFI standard. score4cast
emphasizes the use of strictly proper scores (see [scoringRules]() R
package or Gneiting & Raferty’s landmark 2007 paper) for *probablistic*
forecasts. The EFI format provides a simple but flexible way to express
both ensemble and parametric forecasts in a standard tabular layout.

## Installation

You can install the development version of score4cast from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("eco4cast/score4cast")
```

## Example

A forecast (in standardized format) is scored against a target (in
standardized format):

``` r
library(score4cast)
ex_data <- system.file("extdata/standard-format-examples.R", package="score4cast")
source(ex_data)
#> 
#> Attaching package: 'dplyr'
#> The following objects are masked from 'package:stats':
#> 
#>     filter, lag
#> The following objects are masked from 'package:base':
#> 
#>     intersect, setdiff, setequal, union
#> Loading required package: timechange
#> 
#> Attaching package: 'lubridate'
#> The following objects are masked from 'package:base':
#> 
#>     date, intersect, setdiff, union

scores <- score(ex_forecast, ex_target)
scores
#> # A tibble: 10 × 17
#>    model_id      reference_datetime  site_id datetime            family variable
#>    <chr>         <dttm>              <chr>   <dttm>              <chr>  <chr>   
#>  1 ensemble_team 2022-02-01 00:00:00 BARC    2022-02-02 00:00:00 sample oxygen  
#>  2 ensemble_team 2022-02-01 00:00:00 BARC    2022-02-02 00:00:00 sample tempera…
#>  3 gauss_team    2022-02-01 00:00:00 BARC    2022-02-02 00:00:00 normal oxygen  
#>  4 gauss_team    2022-02-01 00:00:00 BARC    2022-02-02 00:00:00 normal tempera…
#>  5 gauss_team    2022-02-01 00:00:00 BARC    2022-02-03 00:00:00 normal oxygen  
#>  6 gauss_team    2022-02-01 00:00:00 BARC    2022-02-03 00:00:00 normal tempera…
#>  7 gauss_team    2022-02-01 00:00:00 ORNL    2022-02-02 00:00:00 normal oxygen  
#>  8 gauss_team    2022-02-01 00:00:00 ORNL    2022-02-02 00:00:00 normal tempera…
#>  9 gauss_team    2022-02-01 00:00:00 ORNL    2022-02-03 00:00:00 normal oxygen  
#> 10 gauss_team    2022-02-01 00:00:00 ORNL    2022-02-03 00:00:00 normal tempera…
#> # … with 11 more variables: observation <dbl>, crps <dbl>, logs <dbl>,
#> #   mean <dbl>, median <dbl>, sd <dbl>, quantile97.5 <dbl>, quantile02.5 <dbl>,
#> #   quantile90 <dbl>, quantile10 <dbl>, horizon <drtn>
```
