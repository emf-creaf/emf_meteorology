meteo_bitmap_creator <- function(date_to_process, interpolated_meteo_file) {
  # avoid the this and the rest of the pipe if the date is before -366, as
  # we need 16 days of previous meteo
  stopifnot(date_to_process >= (Sys.Date() - 370))

  # we need to create a png for each variable in the interpolated meteo
  meteo_vars <- c(
    "MeanTemperature", "MinTemperature", "MaxTemperature",
    "MeanRelativeHumidity", "MinRelativeHumidity", "MaxRelativeHumidity",
    "Precipitation", "Radiation", "WindSpeed", "PET", "ThermalAmplitude"
  )

  # rounding factor
  # The transformation from variable value to rgb must be done
  # over integers, so we need to multiply by a rounding factor & round to zero
  # decimals
  rounding_factor <- 1000

  # raster platon
  # Interpolated data is stored as POINTs, we need the template raster
  # dimensions and resolution to convert the POINTS to raster and then to RGB
  raster_platon_specs <- readRDS("data-raw/penbal_platon_specs.rds")
  raster_platon <- terra::rast(
    extent = raster_platon_specs$extent,
    resolution = raster_platon_specs$resolution,
    crs = raster_platon_specs$crs,
    nlyrs = length(meteo_vars)
  )

  # # s3 filesystem
  # s3_fs <- S3FileSystem$create(
  #   access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
  #   secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  #   scheme = "https",
  #   endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
  #   region = ""
  # )
  # meteoland_bucket <- s3_fs$cd(
  #   stringr::str_remove(interpolated_meteo_file, "s3://") |>
  #     stringr::str_remove("part-0.parquet")
  # )
  # arrow dataset
  # interpolated_dataset <- open_dataset(meteoland_bucket) |>
  #   sf::st_as_sf()
  interpolated_dataset <- sf::st_read(interpolated_meteo_file, quiet = TRUE)
  # points to cell (pixel) indexes
  cells_index <-
    terra::cells(raster_platon, terra::vect(interpolated_dataset))[, "cell"]

  # assign values to raster platon
  raster_platon[cells_index] <- interpolated_dataset |>
    dplyr::as_tibble() |>
    dplyr::select(
      -geom, -geom_hex, -geom_text,
      -dates, -elevation, -slope, -aspect, -DOY, -WindDirection,
      -TPI, -partition,
      -day, -month, -year
    )

  # assign names to the raster
  names(raster_platon) <-
    names(interpolated_dataset)[
      !names(interpolated_dataset) %in% c(
        "geom", "geom_hex", "geom_text", "dates",
        "elevation", "slope", "aspect", "DOY", "WindDirection",
        "TPI", "partition",
        "day", "month", "year"
      )
    ]

  # write temporal platon to public repository
  ## TODO
  # reduce resolution and project to lat long
  raster_platon_4326 <- raster_platon |>
    terra::aggregate(fact = 4, fun = mean, na.rm = TRUE) |>
    terra::project("epsg:4326")

  # rounding
  raster_platon_4326 <- round(raster_platon_4326 * rounding_factor, 0)

  # color tables
  color_table_gen <- function(meteo_var) {
    scales::col_numeric(
      hcl.colors(256, "ag_GrnYl"),
      c(min(meteo_var, na.rm = TRUE), max(meteo_var, na.rm = TRUE)),
      na.color = "#FFFFFF00", reverse = FALSE, alpha = TRUE
    )(meteo_var)
  }
  color_tables <- terra::values(raster_platon_4326) |>
    dplyr::as_tibble() |>
    dplyr::mutate(dplyr::across(
      .cols = dplyr::everything(),
      .fns = color_table_gen,
      .names = "{.col}_colored"
    )) |>
    as.data.frame()
  # update raster color tables
  purrr::walk(
    .x = 1:length(meteo_vars),
    .f = \(var_index) {
      terra::coltab(raster_platon_4326, layer = var_index) <<-
        color_tables[, c(var_index, length(meteo_vars) + var_index)]
    }
  )

  # create temporal pngs and encode them in base64. Return a tibble with the
  # encoded png and some metadata (extent, palette, min and max values...)
  raster_platon_w <- terra::wrap(raster_platon_4326)
  oopts <- options(future.globals.maxSize = 2.0 * 1e9)  ## 2.0 GB
  on.exit(options(oopts))
  # bitmap_tibble <- purrr::map(
  bitmap_tibble <- furrr::future_map(
    .x = names(raster_platon_4326),
    .options = furrr::furrr_options(seed = TRUE),
    .f = \(meteo_var) {

      raster_platon_unw <- terra::unwrap(raster_platon_w)

      temp_path <- tempfile(meteo_var, fileext = ".png")
      colorized_raster <- terra::colorize(
        raster_platon_unw[[meteo_var]], "rgb",
        NAflag = 255,
        filename = temp_path,
        overwrite = TRUE
      )
      extent_raster <- terra::ext(colorized_raster)
      png_base64_string <- paste0(
        "data:image/png;base64,",
        base64enc::base64encode(temp_path)
      )

      dplyr::tibble(
        date = stringr::str_remove_all(date_to_process, '-'),
        var = meteo_var,
        palette_selected = "ag_GrnYl",
        base64_string = png_base64_string,
        left_ext = as.numeric(extent_raster[1]),
        down_ext = as.numeric(extent_raster[3]),
        right_ext = as.numeric(extent_raster[2]),
        up_ext = as.numeric(extent_raster[4]),
        # min max value of the variable to build the palette/legend later
        min_value = min(color_tables[[meteo_var]], na.rm = TRUE) / rounding_factor,
        max_value = max(color_tables[[meteo_var]], na.rm = TRUE) / rounding_factor
      )
    }
  ) |>
    purrr::list_rbind()

  return(bitmap_tibble)
}

meteo_bitmap_writer <- function(png_tibbles) {
  # s3 filesystem
  s3_fs <- S3FileSystem$create(
    access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
    secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    scheme = "https",
    endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
    region = ""
  )

  # write png tibble
  png_tibbles |>
    purrr::list_rbind() |>
    arrow::write_parquet(
      sink = s3_fs$path("meteoland-spain-app-pngs/daily_interpolated_meteo_bitmaps.parquet"),
      chunk_size = 11
    )

  return("meteoland-spain-app-pngs/daily_interpolated_meteo_bitmaps.parquet")
}