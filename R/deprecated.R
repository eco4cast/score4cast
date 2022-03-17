
## score_it is batch-oriented: takes a large batch of forecast files,
## outputs scores to disk.  This avoid the need to store all forecasts and
## scores in working RAM at the same time.
score_it <- function(targets_file, 
                     forecast_files, 
                     dir = "scores",
                     target_vars = c("oxygen", 
                                     "temperature", 
                                     "richness",
                                     "abundance", 
                                     "nee",
                                     "le", 
                                     "vswc",
                                     "gcc_90",
                                     "rcc_90",
                                     "ixodes_scapularis",
                                     "amblyomma_americanum"),
                     only_forecasts = TRUE
){
  
  dir.create(dir, FALSE, TRUE)
  theme <- strsplit(basename(targets_file), "[-]")[[1]][[1]]
  
  ## Target is processed only once
  target <- 
    readr::read_csv(targets_file, show_col_types = FALSE,
                    lazy = FALSE, progress = FALSE) %>% 
    mutate(theme = theme) %>%
    pivot_target(target_vars)
  
  for(jj in 1:length(forecast_files)){
    message(paste0(jj, "of", length(forecast_files), " ", forecast_files[jj]))
    forecast_file <- forecast_files[jj]
    d <- forecast_file %>%
      read_forecast() %>%
      mutate(filename = forecast_file) %>%
      select_forecasts(only_forecasts) %>%
      pivot_forecast(target_vars) %>%
      crps_logs_score(target) %>% 
      include_horizon() %>%
      write_scores(dir)
    
  }
  
}


## construct filename from columns and write to disk
write_scores <- function(scores, dir = "scores"){
  r <- utils::head(scores,1)
  output <- file.path(dir, 
                      paste0(paste("scores", r$theme, r$time, r$team, sep="-"),
                             ".csv"))
  
  readr::write_csv(scores, output)
  invisible(output)
  
}