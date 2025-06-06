library(dplyr)
library(sf)
library(terra)

# topo files
spain_borders <- mapSpain::esp_get_country(moveCAN = FALSE, year = "2021", epsg = "4326") |>
  sf::st_transform(crs = 25830)

spain_poly <- spain_borders |>
  sf::st_cast("POLYGON") |>
  dplyr::select(geometry) |>
  dplyr::mutate(
    area = sf::st_area(geometry), id = dplyr::row_number(),
    area_km = units::set_units(area, "km^2")
  ) |>
  dplyr::arrange(desc(area)) |>
  dplyr::filter(area_km > units::as_units(10, "km^2")) |>
  # the biggest 12 polygons are the peninsula and the islands
  dplyr::slice(1:12) |>
  sf::st_union()

topo_25_peninsula <- list.files(
  "/srv/emf_data/ftp/emf/datasets/Topography/Sources/Spain/PNOA_MDT25_ETRS89_UTM30",
  pattern = ".tif", full.names = TRUE
)

future::plan(future.callr::callr, workers = 12)

## topo
# fact = 40 are 1km
# fact = 20 are 500m
# fact = 10 are 250m
topo_spain_elev <- topo_25_peninsula |>
  terra::sprc() |>
  terra::merge() |>
  terra::crop(terra::vect(spain_poly), mask = TRUE)

topo_spain <- c(
  topo_spain_elev,
  terra::terrain(
    topo_spain_elev,
    v = c("aspect", "slope", "TPI"), unit = "degrees"
  )
) |>
  terra::aggregate(fact = 20)


peninsula_points <- topo_spain |>
  terra::as.points() |>
  sf::st_as_sf() |>
  purrr::set_names(
    c("elevation", "aspect", "slope", "TPI", "geometry")
  )

# spatial ordering grid
topo_partition_grid <- terra::rast(
  nrows = 15, ncols = 14,
  extent = terra::ext(topo_spain),
  crs = "epsg:25830", vals = 1:(15 * 14),
  names = "partition"
) |>
  terra::as.polygons() |>
  sf::st_as_sf()

topo_arranged <- peninsula_points |>
  sf::st_join(topo_partition_grid) |>
  dplyr::arrange(partition)

duplicated_rows <- topo_arranged |>
  dplyr::select(-partition) |>
  duplicated()

topo_arranged <- topo_arranged |>
  dplyr::filter(!duplicated_rows)

nrow(topo_arranged) == nrow(peninsula_points)
topo_arranged$partition |> unique() |> length()

## Now, topo has the partition (i_step). But some partitions are really
## small, we need to join some partitions. To see which ones is the code
## in testing (below in this same file).
topo_arranged <- topo_arranged |>
  dplyr::mutate(
    partition = dplyr::case_when(
      partition == 1 ~ 15,
      partition == 5 ~ 19,
      partition == 6 ~ 20,
      partition == 7 ~ 21,
      partition == 23 ~ 37,
      partition == 24 ~ 38,
      partition == 25 ~ 39,
      partition == 59 ~ 60,
      partition == 69 ~ 55,
      partition == 81 ~ 80,
      partition == 95 ~ 94,
      partition == 100 ~ 101,
      partition == 114 ~ 115,
      partition == 121 ~ 122,
      partition == 128 ~ 129,
      partition == 142 ~ 143,
      partition == 150 ~ 136,
      partition == 156 ~ 157,
      partition == 170 ~ 171,
      partition == 185 ~ 186,
      partition == 190 ~ 176,
      partition == 199 ~ 200,
      # baleares
      partition %in% c(98, 110, 111, 112, 123, 124, 125, 137) ~ 138,
      # default
      .default = partition
    )
  )
topo_arranged$partition |> unique() |> length()

sf::st_write(
  topo_arranged, "data-raw/penbal_topo_500.gpkg",
  append = FALSE
)

# topo platon raster specs
raster_platon_specs <- list(
  extent = terra::ext(topo_spain) |> as.vector(),
  resolution = terra::res(topo_spain),
  crs = terra::crs(topo_spain)
)
saveRDS(raster_platon_specs, "data-raw/penbal_platon_specs.rds")

# testing
library(ggplot2)
steps <- sort(unique(topo_arranged$partition))

boundaries_arranged <- purrr::map(
  steps,
  .f = \(i_step) {
    # ini <- (i_step - 1) * stepcell + 1
    topo_bbox <- topo_arranged |>
      # sf::st_as_sf() |>
      # dplyr::slice(ini:(ini + stepcell)) |>
      dplyr::filter(partition == i_step) |>
      sf::st_bbox() |>
      sf::st_as_sfc()

    topo_nrow <- topo_arranged |>
      dplyr::filter(partition == i_step) |>
      nrow()

    # topo_sliced |>
    #   dplyr::mutate(bbox = topo_bbox, i_step = i_step)
    dplyr::tibble(i_step = i_step, topo_bbox = topo_bbox, topo_nrow = topo_nrow)
  }
) |>
  purrr::list_rbind() |>
  sf::st_as_sf(sf_column_name = "topo_bbox")

foo <- boundaries_arranged |>
  # dplyr::filter(i_step %in% c(1:25)) |>
  dplyr::mutate(bbox_area = units::drop_units(sf::st_area(topo_bbox))) |>
  ggplot() +
  geom_sf(
    aes(fill = topo_nrow, colour = topo_nrow)
  ) +
  geom_sf_label(aes(label = i_step), size = 6/.pt) #+
  # geom_sf(data = spain_poly, fill = "transparent", color = "red")

ggsave("data-raw/foo.png", foo)
