# Created by use_targets().
# Follow the comments below to fill in this target script.
# Then follow the manual to check and run the pipeline:
#   https://books.ropensci.org/targets/walkthrough.html#inspect-the-pipeline

# Load packages required to define the pipeline:
library(targets)
library(tarchetypes) # Load other packages as needed.
library(crew) # distributed computing

# Set target options:
tar_option_set(
  # Packages that your targets need for their tasks.
  packages = c(
    "tibble", "meteospain", "dplyr", "readr", "arrow",
    "geoarrow",
    "sf", "stringr"
  ),
  # Default format to qs instead of rds
  format = "qs",
  # Memory to transient to avoid having all the objects of all targets loaded
  memory = "transient",
  # iteration mode default to list
  iteration = "list"
  # Set other options as needed.
)

# Run the R scripts in the R/ folder with your custom functions:
tar_source()
# tar_source("other_functions.R") # Source other scripts as needed.

# Read the environment file needed for databases and API keys
readRenviron("/home/vgranda/envvars/lfc_development_env")
# readRenviron("/data/25_secrets/lfc_development_env")

# future plan
future::plan(future.callr::callr, workers = 12)

# dates
dates_to_process <- seq(Sys.Date() - 385, Sys.Date() - 5, by = "day")
# dates_to_process <- seq(Sys.Date() - 385, Sys.Date() - 385, by = "day")
# topo path
raw_topo_paths <- file.path("data-raw", "penbal_topo_500.gpkg")

# emf_meteorology target list
list(
  # dates input
  tar_target(dates, dates_to_process),
  # topo path
  tar_target(topo_paths, raw_topo_paths),
  # aemet, dinamic branching for all the dates
  tar_target(
    daily_aemet, aemet_getter(dates),
    pattern = map(dates),
    # continue on error, return NULL for those failed dates
    error = "null"
  ),
  # meteocat, dinamic branching for all the dates
  tar_target(
    daily_meteocat, meteocat_getter(dates),
    pattern = map(dates),
    # continue on error, return NULL for those failed dates
    error = "null"
  ),
  # meteogalicia, dinamic branching for all the dates
  tar_target(
    daily_meteogalicia, meteogalicia_getter(dates),
    pattern = map(dates),
    # continue on error, return NULL for those failed dates
    error = "null"
  ),
  # ria, dinamic branching for all the dates
  tar_target(
    daily_ria, ria_getter(dates),
    pattern = map(dates),
    # continue on error, return NULL for those failed dates
    error = "null"
  ),
  # join them
  tar_target(
    joined_meteo,
    meteo_formatter(daily_aemet, daily_meteocat, daily_meteogalicia, daily_ria),
    pattern = map(daily_aemet, daily_meteocat, daily_meteogalicia, daily_ria),
    # continue on error, return NULL for those failed dates
    error = "null"
  ),
  # write parquet files branched
  tar_target(
    raw_meteo_files,
    meteo_raw_writer(joined_meteo),
    pattern = map(joined_meteo),
    format = "file",
    # continue on error, return NULL for those failed dates
    error = "null"
  ),
  # create interpolators and calibrate them
  tar_target(
    calibrations,
    meteo_interpolator_calibrator(dates, raw_meteo_files, topo_paths),
    pattern = cross(topo_paths, map(dates, raw_meteo_files)),
    error = "null"
  ),
  # interpolate and write result branched
  tar_target(
    interpolated_meteo,
    meteo_interpolator(dates, calibrations, topo_paths),
    pattern = cross(topo_paths, map(dates, calibrations)),
    format = "file",
    error = "null"
  ),
  tar_target(
    interpolated_parquet_files,
    meteo_parquet_writer(interpolated_meteo),
    pattern = map(interpolated_meteo),
    # format = "file",
    error = "null"
  ),
  tar_target(
    cross_validations,
    meteo_cross_validator(calibrations),
    pattern = map(calibrations),
    error = "null"
  ),
  tar_target(
    cv_tables, cross_validations_postprocessor(cross_validations),
    pattern = map(cross_validations),
    error = "null"
  ),
  tar_target(
    cv_table_file, cross_validation_writer(cv_tables),
    # format = "file",
    error = "null"
  ),
  # bitmaps creation
  tar_target(
    png_tibbles,
    meteo_bitmap_creator(dates, interpolated_meteo),
    pattern = map(dates, interpolated_meteo),
    # continue on error, return NULL
    error = "null"
  ),
  # bitmap parquet file
  tar_target(
    bitmaps_table_file,
    meteo_bitmap_writer(png_tibbles),
    # format = "file",
    # continue on error, return NULL
    error = "null"
  )
)
