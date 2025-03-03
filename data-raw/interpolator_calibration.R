library(meteoland)
library(stars)
library(lubridate)
library(arrow)
library(geoarrow)
library(furrr)
library(ggplot2)
library(ntfy)

raw_meteo_folder <- "daily_raw_meteo"
topo_path <- "data-raw/peninsula_topo_500.gpkg"
dates <- seq(Sys.Date() - 364, Sys.Date() - 5, by = "month") |>
  lubridate::floor_date("month")

interpolator_calibration <- function(date_ini, raw_meteo_folder, topo_path) {
  # debug
  # browser()

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

  # interpolate meteo to topo
  # ncells <- sf::st_read(
  #   topo_path, query = "SELECT COUNT(*) FROM \"topo\"", quiet = TRUE
  # )[[1]]
  # # make chunks of {stepcell} pixels based on 12 cores and (12*n)
  # # processes per core
  # stepcell <- ceiling(ncells / 12 / (12 * 1))
  # # calculate the steps necessary
  # steps <- ceiling(ncells / stepcell) # number of chunks
  
  # steps are given by the spatially ordered topo, variable "partition"
  steps <- sf::st_read(
    topo_path,
    query = "SELECT DISTINCT partition FROM \"peninsula_topo_500\"",
    quiet = TRUE
  )$partition

  # date_fin <- lubridate::ceiling_date(date_ini, "month")
  date_fin <- date_ini + 15

  calibration_month <- furrr::future_map(
  # calibration_month <- purrr::map(
    steps,
    .f = \(i_step) {
      topo_sliced <- topo_path |>
        sf::st_read(
          quiet = TRUE,
          query = glue::glue(
            "SELECT * FROM \"peninsula_topo_500\" WHERE partition = {i_step}"
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
          # N_seq = seq(1, 151, by = 10),
          # alpha_seq = seq(0.5, 100.5, by = 10),
          N_seq = c(1, seq(100, 150, by = 10)),
          alpha_seq = seq(0.5, 50.5, by = 10),
          verbose = FALSE
        )
      tmax_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "MaxTemperature",
          # N_seq = seq(1, 151, by = 10),
          # alpha_seq = seq(0.5, 100.5, by = 10),
          N_seq = c(1, seq(1, 151, by = 10)),
          alpha_seq = seq(0.5, 50.5, by = 10),
          verbose = FALSE
        )
      tdew_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "DewTemperature",
          # N_seq = seq(1, 151, by = 10),
          # alpha_seq = seq(0.5, 100.5, by = 10),
          N_seq = c(1, 10, 30, 70, 150),
          alpha_seq = seq(0.5, 60.5, by = 10),
          verbose = FALSE
        )
      prec_event_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "PrecipitationEvent",
          # N_seq = seq(1, 151, by = 10),
          # alpha_seq = seq(0.5, 100.5, by = 10),
          N_seq = c(10, 20, 30),
          alpha_seq = seq(20, 120, by = 10),
          verbose = FALSE
        )
      prec_amount_cal <- interpolator |>
        meteoland::interpolator_calibration(
          update_interpolation_params = FALSE,
          variable = "PrecipitationAmount",
          # N_seq = seq(1, 151, by = 10),
          # alpha_seq = seq(0.5, 100.5, by = 10),
          N_seq = c(10, 150),
          alpha_seq = c(1, 10, 20, 30),
          verbose = FALSE
        )

      # save calibration params
      monthly_calibration_params_table <- dplyr::tibble(
        month = lubridate::month(date_ini),
        year = lubridate::year(date_ini),
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
        topo_ncells = nrow(topo_sliced)
      )

      return(monthly_calibration_params_table)
    },
    .options = furrr::furrr_options(
      packages = c("sf", "dplyr", "meteoland", "tidyr", "arrow", "geoarrow"),
      chunk_size = 3
    )
  ) |>
    purrr::list_rbind()

  saveRDS(
    calibration_month,
    paste0(
      "data-raw/calibration_table_",
      lubridate::year(date_ini), "_", lubridate::month(date_ini),
      ".rds"
    )
  )

  return(calibration_month)
}

# future plan
future::plan(future.callr::callr, workers = 12)

calibration_params_table <- purrr::map(
  dates,
  .f = \(date_ini) {
    cli::cli_inform("Processing {date_ini}:")
    tictoc::tic()
    res <- interpolator_calibration(date_ini, raw_meteo_folder, topo_path)
    tictoc::toc()
    cli::cli_inform("{date_ini} done")
    return(res)
  }
) |>
  purrr::list_rbind()

saveRDS(calibration_params_table, "data-raw/calibrations_table.rds")

summary_tmin <- calibration_params_table |>
  dplyr::distinct() |>
  dplyr::mutate(topo_bbox = sf::st_as_sfc(topo_bbox)) |>
  sf::st_as_sf() |>
  # dplyr::filter(n_stations < 51) |>
  ggplot() +
  geom_sf(aes(fill = tmin_n, color = tmin_n), alpha = 1) +
  facet_wrap(vars(year, month)) +
  theme_minimal()
ggsave("data-raw/summary_tmin.png", summary_tmin)

ntfy_send(
  message = "Calibration space test done",
  tags = tags$bell,
  priority = 5,
  topic = "transient"
)

# calibration_params_table <-
#   list.files("data-raw", pattern = "table", full.names = TRUE) |>
#   purrr::map(.f = readRDS) |>
#   purrr::list_rbind()
calibration_params_table <- readRDS("data-raw/calibrations_table.rds")

library(ggplot2)
ggplot(calibration_params_table) +
  geom_hex(aes(x = tmin_n, y = tmin_alpha)) +
  facet_wrap(vars(year, month))

ggplot(calibration_params_table) +
  geom_hex(aes(x = tmax_n, y = tmax_alpha)) +
  facet_wrap(vars(year, month))

ggplot(calibration_params_table) +
  geom_hex(aes(x = tdew_n, y = tdew_alpha)) +
  facet_wrap(vars(year, month))

ggplot(calibration_params_table) +
  geom_hex(aes(x = prec_event_n, y = prec_event_alpha)) +
  facet_wrap(vars(year, month))

ggplot(calibration_params_table) +
  geom_hex(aes(x = prec_amount_n, y = prec_amount_alpha)) +
  facet_wrap(vars(year, month))

ggplot(calibration_params_table) +
  geom_histogram(aes(x = prec_amount_n))
ggplot(calibration_params_table) +
  geom_histogram(aes(x = prec_amount_alpha))

ggplot(calibration_params_table) +
  geom_histogram(aes(x = prec_event_n))
ggplot(calibration_params_table) +
  geom_histogram(aes(x = prec_event_alpha))

ggplot(calibration_params_table) +
  geom_histogram(aes(x = tmin_n))
ggplot(calibration_params_table) +
  geom_histogram(aes(x = tmin_alpha, after_stat(ncount)))

ggplot(calibration_params_table) +
  geom_histogram(aes(x = tmax_n))
ggplot(calibration_params_table) +
  geom_histogram(aes(x = tmax_alpha))

ggplot(calibration_params_table) +
  geom_histogram(aes(x = tdew_n))
ggplot(calibration_params_table) +
  geom_histogram(aes(x = tdew_alpha))

calibration_params_table |>
  # dplyr::filter(n_stations < 50) |>
  # dplyr::select(
  #   i_step, n_stations, topo_ncells,
  #   tdew_mae,
  #   year, month,
  #   buffer, topo_bbox
  # ) |>
  dplyr::distinct() |>
  dplyr::mutate(topo_bbox = sf::st_as_sfc(topo_bbox)) |>
  sf::st_as_sf() |>
  # dplyr::filter(n_stations < 51) |>
  ggplot() +
  geom_sf(aes(fill = tmax_mae, color = tmax_mae), alpha = 1) +
  facet_wrap(vars(year, month)) +
  theme_minimal()


ggplot(calibration_params_table) +
  geom_histogram(aes(x = tmin_mae), binwidth = 0.25)
ggplot(calibration_params_table) +
  geom_histogram(aes(x = tmax_mae), binwidth = 0.25)
ggplot(calibration_params_table) +
  geom_histogram(aes(x = tdew_mae), binwidth = 0.25)
ggplot(calibration_params_table) +
  geom_histogram(aes(x = prec_amount_mae), binwidth = 0.25)
ggplot(calibration_params_table) +
  geom_histogram(aes(x = prec_event_mae), binwidth = 0.01)
