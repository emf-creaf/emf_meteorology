meteo_raw_writer <- function(daily_meteo) {
  # write parquet file as file on the daily meteo dataset
  daily_meteo <- daily_meteo |>
    dplyr::as_tibble() |>
    dplyr::mutate(
      day = lubridate::day(dates),
      month = lubridate::month(dates),
      year = lubridate::year(dates),
      dates = as.character(dates)
    )
  daily_meteo |>
    write_dataset(
      path = "daily_raw_meteo", format = "parquet",
      partitioning = c("year", "month", "day"),
      existing_data_behavior = "overwrite"
    )
  # files vector to return for targets `target = "file"`
  file.path(
    "daily_raw_meteo",
    paste0("year=", unique(daily_meteo[["year"]])),
    paste0("month=", unique(daily_meteo[["month"]])),
    paste0("day=", unique(daily_meteo[["day"]])),
    "part-0.parquet"
  )
}

meteo_parquet_writer <- function(gpkg_file) {
  # avoid the this and the rest of the pipe if the date is before -366, as
  # we need 16 days of previous meteo. In this case, missing gpkg
  stopifnot(!is.null(gpkg_file))
  stopifnot(file.exists(gpkg_file))

  # browser()
  interpolated_day <- sf::st_read(gpkg_file, quiet = TRUE)
  interpolated_day |>
    write_dataset(
      path = "/srv/emf_data/fileserver/parquet/daily_interpolated_meteo",
      format = "parquet",
      partitioning = c("year", "month", "day"),
      existing_data_behavior = "overwrite",
      min_rows_per_group = 50000
    )

  part_parquet_file_name <- paste0(
    "/srv/emf_data/fileserver/parquet/daily_interpolated_meteo/",
    paste0("year=", unique(interpolated_day[["year"]]), "/"),
    paste0("month=", unique(interpolated_day[["month"]]), "/"),
    paste0("day=", unique(interpolated_day[["day"]]), "/"),
    "part-0.parquet"
  )

  cli::cli_inform(c(
    "v" = "Finished parquet creation.",
    "i" = "{.path {part_parquet_file_name}} created in fileserver : {file.exists(part_parquet_file_name)}"
  ))

  return(part_parquet_file_name)
}