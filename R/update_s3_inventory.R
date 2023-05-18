

update_s3_inventory <- function(bucket, inventory) {
  paths <- bucket$ls(recursive = TRUE)
  full_path <- stringi::stri_detect_fixed(paths, ".")
  parts <- stringi::stri_split(paths[full_path], fixed="/", simplify=TRUE)
  parts <- tibble::as_tibble(parts, .name_repair = "universal")
  dest <- inventory$path(bucket$base_path)
  arrow::write_dataset(parts, dest)
  
}

#' helper method to run an update of EFI inventory
#' 
#' @param bucket_name name of one the EFI buckets. 
#' `neon4cast-forecasts` is the most important one to inventory and is actively
#' used in scoring.  Other buckets can easily open at the repository root.
#' @param endpoint endpoint address, default is good.
#' @export
efi_update_inventory <- function(bucket_name = "neon4cast-forecasts",
                                 endpoint = "data.ecoforecast.org") {
  bucket <- arrow::s3_bucket(bucket_name,
                             endpoint_override = endpoint,
                             anonymous = TRUE)
  inventory <- arrow::s3_bucket("neon4cast-inventory",
                                endpoint_override = endpoint,
                                access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
                                secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"))
  
  update_s3_inventory(bucket, inventory)
}

