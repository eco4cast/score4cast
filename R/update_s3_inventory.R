update_s3_inventory <- function(bucket, inventory) {
  paths <- bucket$ls(recursive = TRUE)
  full_path <- stringi::stri_detect_fixed(paths, ".")
  parts <- stringi::stri_split(paths[full_path], fixed="/", simplify=TRUE)
  parts <- tibble::as_tibble(parts, .name_repair = "universal")
  dest <- inventory$path(bucket$base_path)
  arrow::write_dataset(parts, dest)
  
}


efi_update_inventory <- function() {
  bucket <- arrow::s3_bucket("neon4cast-forecasts",
                             endpoint_override = "data.ecoforecast.org",
                             anonymous = TRUE)
  
  
  inventory <- arrow::s3_bucket("neon4cast-inventory",
                                endpoint_override = "data.ecoforecast.org",
                                access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
                                secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"))
  
  update_s3_inventory(bucket, inventory)
}

