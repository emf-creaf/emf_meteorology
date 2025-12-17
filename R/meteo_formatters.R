meteo_formatter <- function(aemet, meteocat, meteogalicia, ria) {

  # check args
  if (is.null(aemet)) {
    cli::cli_abort("AEMET meteo is missing, unable to format and continue")
  }
  # polygon to remove Canary islands and Ceuta/Melilla
  bbox_peninsula <- sf::st_bbox(
    c(xmin = -15, xmax = 5, ymin = 35, ymax = 44.4),
    crs = sf::st_crs(4326)
  ) |>
    sf::st_as_sfc()
  # format to meteoland, filter peninsula
  dplyr::bind_rows(aemet, meteocat, meteogalicia, ria) |>
    meteoland::meteospain2meteoland(complete = TRUE) |>
    sf::st_filter(bbox_peninsula)
}