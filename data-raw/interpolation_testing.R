library(targets)
calibration <- tar_read(calibrations_e4158ccfe74f5ad6) |>
  # problematic in this day are 170 and 156
  dplyr::filter(i_step %in% c(156, 170))

date_fin <- as.Date("2025-03-16")

topo_path <- file.path("data-raw", "penbal_topo_500.gpkg")

meteo_interpolator <- function(date_fin, calibration, topo_path) {
  browser()
  # avoid the this and the rest of the pipe if the date is before -366, as
  # we need 16 days of previous meteo
  stopifnot(date_fin >= (Sys.Date() - 370))

  cli::cli_inform(c("i" = "Interpolating {.date {date_fin}}"))

  # steps are given by the spatially ordered topo, variable "partition"
  steps <- sf::st_read(
    topo_path,
    query = "SELECT DISTINCT partition FROM \"penbal_topo_500\"",
    quiet = TRUE
  )$partition

  # mirai preparation
  mirai::daemons(12)
  mirai::everywhere(
    {},
    calibration = calibration, topo_path = topo_path, date_fin = date_fin
  )
  withr::defer(mirai::daemons(0))

  # interpolated_day <- purrr::map(
  # interpolated_day <- furrr::future_map(
  interpolated_day <- mirai::mirai_map(
    steps,
    .f = \(i_step) {
      topo_sliced <- topo_path |>
        sf::st_read(
          quiet = TRUE,
          query = glue::glue(
            "SELECT * FROM \"penbal_topo_500\" WHERE partition = {i_step}"
          )
        )

      interpolator <- calibration |>
        dplyr::filter(i_step == !!i_step) |>
        dplyr::pull(interpolator_path) |>
        meteoland::read_interpolator() |>
        sf::st_transform(sf::st_crs(topo_sliced))

      meteoland::interpolate_data(
        topo_sliced, interpolator, as.Date(date_fin),
        ignore_convex_hull_check = TRUE, verbose = TRUE
      ) |>
        dplyr::as_tibble() |>
        tidyr::unnest("interpolated_data") |>
        dplyr::mutate(
          ThermalAmplitude = MaxTemperature - MinTemperature,
          day = lubridate::day(as.Date(date_fin)),
          month = lubridate::month(as.Date(date_fin)),
          year = lubridate::year(as.Date(date_fin)),
          geom_hex = sf::st_as_binary(geom, hex = TRUE),
          geom_text = sf::st_as_text(geom)
        )
    }# ,
    # .options = furrr::furrr_options(
    #   packages = c("sf", "dplyr", "meteoland", "tidyr", "arrow", "geoarrow"),
    #   # scheduling = 2
    #   chunk_size = 3
    # )
  ) |>
    purrr::list_rbind()

  # write interpolated meteo (POINTS geopackge)
  gpkg_file_name <- paste0(
    "/srv/emf_data/fileserver/gpkg/daily_interpolated_meteo/",
    stringr::str_remove_all(unique(interpolated_day[["dates"]]), "-"),
    ".gpkg"
  )
  sf::st_write(interpolated_day, gpkg_file_name, delete_layer = TRUE, quiet = TRUE)

  cli::cli_inform(c(
    "v" = "Finished interpolation for {.date {date_fin}}"
  ))

  # return the filename
  return(gpkg_file_name)
}

meteo_interpolator(date_fin, calibration, topo_path)
