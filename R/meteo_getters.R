meteocat_getter <- function(date) {
  # key
  meteocat_key <- Sys.getenv("METEOCAT")
  # api daily options
  meteocat_opts <- meteocat_options(
    "daily",
    start_date = date,
    api_key = meteocat_key
  )
  # check not today
  if (date == Sys.Date()) {
    cli::cli_abort("Date can not be today due to incomplete data")
  }
  # meteo download
  meteocat_meteo <- try(
    get_meteo_from("meteocat", meteocat_opts) |>
      # fix any other date in aemet_meteo
      dplyr::filter(lubridate::day(timestamp) == lubridate::day(date)),
    silent = TRUE
  )
  # check if data was downloaded
  if (inherits(meteocat_meteo, "try-error")) {
    cli::cli_abort(c(
      "x" = "Unable to download data for {.date {date}} from Meteocat service",
      "i" = meteocat_meteo[1]
    ))
  }
  # return raw data
  meteocat_meteo
}

aemet_getter <- function(date) {
  
  # custom dates for caching results (avoid calling the API so many times)
  options_dates <- c(
    as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-16")),
    lubridate::ceiling_date(date, "month") - 1
  )

  if (lubridate::day(date) <= 15) {
    options_dates <- c(
      as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-01")),
      as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-15"))
    )
  }

  # key
  aemet_key <- Sys.getenv("AEMET")
  # api daily options
  aemet_opts <- aemet_options(
    "daily",
    start_date = options_dates[1], end_date = options_dates[2],
    api_key = aemet_key
  )
  # Check not 4 days
  if (date >= (Sys.Date() - 4)) {
    cli::cli_abort("Date must be at least 4 days before today due to missing data")
  }
  # meteo download
  aemet_meteo <- try(
    get_meteo_from("aemet", aemet_opts) |>
      # fix any other date in aemet_meteo
      dplyr::filter(lubridate::day(timestamp) == lubridate::day(date)),
    silent = TRUE
  )
  # check if data was downloaded
  if (inherits(aemet_meteo, "try-error")) {
    cli::cli_abort(c(
      "x" = "Unable to download data for {.date {date}} from AEMET service",
      "i" = aemet_meteo[1]
    ))
  }
  # return raw data
  aemet_meteo
}

meteogalicia_getter <- function(date) {
  # custom dates for caching results (avoid calling the API so many times)
  options_dates <- c(
    as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-16")),
    lubridate::ceiling_date(date, "month") - 1
  )

  if (lubridate::day(date) <= 15) {
    options_dates <- c(
      as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-01")),
      as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-15"))
    )
  }

  # api options
  meteogalicia_opts <- meteogalicia_options(
    resolution = 'daily',
    start_date = options_dates[1], end_date = options_dates[2]
  )

  # meteo download
  meteogalicia_meteo <- try(
    get_meteo_from("meteogalicia", meteogalicia_opts) |>
      # fix any other date in aemet_meteo
      dplyr::filter(lubridate::day(timestamp) == lubridate::day(date)),
    silent = TRUE
  )
  # check if data was downloaded
  if (inherits(meteogalicia_meteo, "try-error")) {
    cli::cli_abort(c(
      "x" = "Unable to download data for {.date {date}} from MeteoGalicia service",
      "i" = meteogalicia_meteo[1]
    ))
  }
  # return raw data
  meteogalicia_meteo
}

ria_getter <- function(date) {
  # custom dates for caching results (avoid calling the API so many times)
  options_dates <- c(
    as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-16")),
    lubridate::ceiling_date(date, "month") - 1
  )

  if (lubridate::day(date) <= 15) {
    options_dates <- c(
      as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-01")),
      as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-15"))
    )
  }

  # api options
  ria_opts <- ria_options(
    resolution = 'daily',
    start_date = options_dates[1], end_date = options_dates[2]
  )

  # meteo download
  ria_meteo <- try(
    get_meteo_from("ria", ria_opts) |>
      # fix any other date in aemet_meteo
      dplyr::filter(lubridate::day(timestamp) == lubridate::day(date)),
    silent = TRUE
  )
  # check if data was downloaded
  if (inherits(ria_meteo, "try-error")) {
    cli::cli_abort(
      "x" = "Unable to download data for {.date {date}} from ria service",
      "i" = ria_meteo[1]
    )
  }
  # return raw data
  ria_meteo
}


#### DEBUG versions ####
aemet_getter_debug <- function(date) {
  
  # custom dates for caching results (avoid calling the API so many times)
  options_dates <- c(
    as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-16")),
    lubridate::ceiling_date(date, "month") - 1
  )

  if (lubridate::day(date) <= 15) {
    options_dates <- c(
      as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-01")),
      as.Date(paste0(lubridate::year(date), "-", lubridate::month(date), "-15"))
    )
  }

  # key
  aemet_key <- Sys.getenv("AEMET")
  # api current day options
  aemet_opts <- aemet_options("current_day", api_key = aemet_key)
  # api daily options
  if (date != (Sys.Date() - 1)) {
    if (date < (Sys.Date() - 4)) {
      aemet_opts <- aemet_options(
        "daily",
        start_date = options_dates[1], end_date = options_dates[2],
        api_key = aemet_key
      )
    } else {
      cli::cli_abort("No hourly or daily data availble for {as.character(date)}")
    }
  }
  # browser()
  # meteo download
  aemet_meteo <- try(
    get_meteo_from("aemet", aemet_opts) |>
      # fix any other date in aemet_meteo
      dplyr::filter(lubridate::day(timestamp) == lubridate::day(date)),
    silent = TRUE
  )
  # check if data was downloaded
  if (inherits(aemet_meteo, "try-error")) {
    cli::cli_alert_warning(aemet_meteo)
    cli::cli_abort("Unable to download data for {.date {date}} from AEMET service")
  }
  # return raw data
  aemet_meteo
}