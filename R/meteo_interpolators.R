meteo_interpolator_calibrator <- function(date_fin, raw_meteo_file, topo_path) {

  # avoid the this and the rest of the pipe if the date is before -366, as
  # we need 16 days of previous meteo
  stopifnot(date_fin >= (Sys.Date() - 370))

  # If the date is correct, then we continue the process
  cli::cli_inform(c("i" = "Creating calibrated interpolators for {.date {date_fin}}"))

  raw_meteo_folder <- stringr::str_split_i(raw_meteo_file, "/", 1)

  # black holes filtering
  black_holes_connection <- open_dataset(raw_meteo_folder) |>
    dplyr::select(stationID, Precipitation, WindSpeed, geometry) |>
    dplyr::group_by(stationID) |>
    dplyr::summarise(
      Precipitation_sum = sum(Precipitation, na.rm = TRUE),
      WindSpeed_sum = sum(WindSpeed, na.rm = TRUE)
    )

  stations_zero_prec <- black_holes_connection |>
    dplyr::filter(Precipitation_sum <= 0) |>
    dplyr::pull(stationID, as_vector = TRUE)

  stations_zero_wind <- black_holes_connection |>
    dplyr::filter(WindSpeed_sum <= 0) |>
    dplyr::pull(stationID, as_vector = TRUE)

  # steps are given by the spatially ordered topo, variable "partition"
  steps <- sf::st_read(
    topo_path,
    query = "SELECT DISTINCT partition FROM \"penbal_topo_500\"",
    quiet = TRUE
  )$partition

  # interpolator dates
  date_ini <- date_fin - 16

  # mirai preparation
  mirai::daemons(5)
  withr::defer(mirai::daemons(0))
  mirai::everywhere(
    {
      library(sf)
      library(dplyr)
      library(meteoland)
      library(arrow)
      library(geoarrow)
    }
  )

  # interpolator creation and calibration for each topo slice
  mirai::mirai_map(
    steps,
    .f = \(i_step) {
      topo_sliced <- topo_path |>
        sf::st_read(
          quiet = TRUE,
          query = glue::glue(
            "SELECT * FROM \"penbal_topo_500\" WHERE partition = {i_step}"
          )
        )

      topo_bbox <- topo_sliced |>
        sf::st_bbox() |>
        sf::st_as_sfc()
      buffer <- topo_bbox |>
        sf::st_buffer(99000)

      interpolator <- open_dataset(raw_meteo_folder) |>
        dplyr::filter(dates >= date_ini, dates <= date_fin) |>
        dplyr::select(
          dates, stationID, elevation, slope, aspect,
          dplyr::matches("Temperature", ignore.case = FALSE),
          dplyr::matches("RelativeHumidity", ignore.case = FALSE),
          dplyr::matches("Precipitation", ignore.case = FALSE),
          dplyr::matches("Wind", ignore.case = FALSE),
          dplyr::matches("Radiation", ignore.case = FALSE),
          geometry
        ) |>
        dplyr::mutate(
          Precipitation = dplyr::if_else(
            stationID %in% stations_zero_prec,
            NA_real_, as.numeric(Precipitation)
          ),
          WindSpeed = dplyr::if_else(
            stationID %in% stations_zero_wind,
            NA_real_, as.numeric(WindSpeed)
          ),
          WindDirection = dplyr::if_else(
            stationID %in% stations_zero_wind,
            NA_real_, as.numeric(WindDirection)
          ),
          dates = as.Date(dates)
        ) |>
        sf::st_as_sf() |>
        # convert to topo projection. Meteoland transforms the topo to the
        # interpolator crs, but we want to maintain the topo always in its crs.
        # So we transform first the meteo and build the interpolator with this
        # crs.
        sf::st_transform(sf::st_crs(topo_sliced)) |>
        # we only need the stations covered by the topo (with a buffer).
        # That way the `Initial_Rp` parameter is calculated correctly.
        sf::st_filter(buffer) |>
        # this mutate is after collecting the parquet data because last
        # is not an expression supported in Arrow (for now)
        dplyr::mutate(
          # ensure geometry and elevation are the most recent values
          elevation = dplyr::last(elevation),
          aspect = dplyr::last(aspect),
          slope = dplyr::last(slope),
          geometry = dplyr::last(geometry),
          .by = stationID
        ) |>
        meteoland::create_meteo_interpolator(verbose = FALSE)

      tmin_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "MinTemperature",
          N_seq = seq(20, 160, by = 20),
          alpha_seq = seq(10, 50, by = 10),
          verbose = FALSE
        )
      tmax_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "MaxTemperature",
          N_seq = seq(20, 160, by = 20),
          alpha_seq = seq(10, 50, by = 10),
          verbose = FALSE
        )
      tdew_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "DewTemperature",
          N_seq = c(10, 30, 130, 150),
          alpha_seq = seq(0.5, 60.5, by = 10),
          verbose = FALSE
        )
      prec_event_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "PrecipitationEvent",
          N_seq = c(10, 20, 30),
          alpha_seq = seq(20, 100, by = 20),
          verbose = FALSE
        )
      prec_amount_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "PrecipitationAmount",
          N_seq = c(10, 50, 120, 150),
          alpha_seq = c(1, 10, 20, 30),
          verbose = FALSE
        )

      calibrated_interpolator <- interpolator |>
        meteoland::set_interpolation_params(
          verbose = FALSE,
          params = list(
            N_MinTemperature = tmin_cal$N,
            alpha_MinTemperature = tmin_cal$alpha,
            N_MaxTemperature = tmax_cal$N,
            alpha_MaxTemperature = tmax_cal$alpha,
            N_DewTemperature = tdew_cal$N,
            alpha_DewTemperature = tdew_cal$alpha,
            N_PrecipitationEvent = prec_event_cal$N,
            alpha_PrecipitationEvent = prec_event_cal$alpha,
            N_PrecipitationAmount = prec_amount_cal$N,
            alpha_PrecipitationAmount = prec_amount_cal$alpha
          )
        )
      interpolator_path <- glue::glue(
        "interpolators/interpolator_{lubridate::year(date_fin)}_{lubridate::month(date_fin)}_{lubridate::day(date_fin)}_{i_step}.nc"
      )
      ### TODO
      # Due to a "bug" in ncmeta (called through ncdfgeom), if we use
      # UTM coordinates when writing the interpolator, crs is lost. So
      # we save the interpolator as latlong, and remember to transform it
      # to 25830 when reading it later
      suppressMessages(
        meteoland::write_interpolator(
          sf::st_transform(calibrated_interpolator, 4326),
          interpolator_path, .overwrite = TRUE
        )
      )

      # save calibration params
      dplyr::tibble(
        day = lubridate::day(date_fin),
        month = lubridate::month(date_fin),
        year = lubridate::year(date_fin),
        i_step = i_step,
        n_stations = length(
          stars::st_get_dimension_values(interpolator, "station")
        ),
        tmin_n = tmin_cal$N,
        tmax_n = tmax_cal$N,
        tdew_n = tdew_cal$N,
        prec_event_n = prec_event_cal$N,
        prec_amount_n = prec_amount_cal$N,
        tmin_alpha = tmin_cal$alpha,
        tmax_alpha = tmax_cal$alpha,
        tdew_alpha = tdew_cal$alpha,
        prec_event_alpha = prec_event_cal$alpha,
        prec_amount_alpha = prec_amount_cal$alpha,
        tmin_mae = tmin_cal$minMAE,
        tmax_mae = tmax_cal$minMAE,
        tdew_mae = tdew_cal$minMAE,
        prec_event_mae = prec_event_cal$minMAE,
        prec_amount_mae = prec_amount_cal$minMAE,
        buffer = sf::st_as_text(buffer),
        topo_bbox = sf::st_as_text(topo_bbox),
        topo_ncells = nrow(topo_sliced),
        interpolator_path = interpolator_path
      )
    },
    topo_path = topo_path, raw_meteo_folder = raw_meteo_folder,
    date_ini = date_ini, date_fin = date_fin
  )[] |>
    purrr::list_rbind()
}


meteo_interpolator <- function(date_fin, calibration, topo_path) {
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
  mirai::daemons(5)
  withr::defer(mirai::daemons(0))
  mirai::everywhere(
    {
      library(sf)
      library(dplyr)
      library(meteoland)
      library(tidyr)
      library(arrow)
      library(geoarrow)
    },
    topo_path = topo_path, calibration = calibration, date_fin = date_fin
  )

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
        ignore_convex_hull_check = TRUE, verbose = FALSE
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
    #   scheduling = FALSE
    # )
  )[] |>
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

meteo_cross_validator <- function(calibration) {

  stopifnot(!is.null(calibration))

  # mirai preparation
  mirai::daemons(5)
  withr::defer(mirai::daemons(0))
  mirai::everywhere({ library(meteoland) })

  paths <- calibration |>
    dplyr::pull(interpolator_path) |>
    purrr::set_names()
  # furrr::future_map(
  mirai::mirai_map(
    paths,
    .f = \(i_path) {
      meteoland::read_interpolator(i_path) |>
        meteoland::interpolation_cross_validation(verbose = FALSE)
    }# ,
    # .options = furrr::furrr_options(
    #   packages = c("sf", "dplyr", "meteoland", "tidyr", "arrow", "geoarrow"),
    #   scheduling = FALSE
    # )
  )[]
}