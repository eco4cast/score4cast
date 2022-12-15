score_schema <- function() {
  arrow::schema(
    datetime = arrow::timestamp("us", timezone="UTC"), 
    family=arrow::string(),
    variable = arrow::string(), 
    prediction=arrow::float64(), 
    reference_datetime=arrow::string(),
    site_id=arrow::string(),
    model_id = arrow::string(),
    observation=arrow::float64(),
    crps = arrow::float64(),
    logs = arrow::float64(),
    mean = arrow::float64(),
    median = arrow::float64(),
    sd = arrow::float64(),
    quantile97.5 = arrow::float64(),
    quantile02.5 = arrow::float64(),
    quantile90 = arrow::float64(),
    quantile10= arrow::float64()
  )
}


forecast_schema <- function() { 
  arrow::schema(target_id = arrow::string(), 
                datetime = arrow::timestamp("us", timezone = "UTC"), 
                parameter=arrow::string(),
                variable = arrow::string(), 
                prediction=arrow::float64(),
                family=arrow::string(),
                reference_datetime=arrow::string(),
                site_id=arrow::string(),
                model_id = arrow::string(),
                date=arrow::string()
  )
}