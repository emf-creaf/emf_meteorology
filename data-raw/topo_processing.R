library(dplyr)
library(sf)
library(terra)

# topo files
spain_borders <- mapSpain::esp_get_country(moveCAN = FALSE, year = "2021", epsg = "4326") |>
  sf::st_transform(crs = 25830)

peninsula_only <- spain_borders |>
  sf::st_cast("POLYGON") |>
  dplyr::select(geometry) |>
  dplyr::mutate(area = sf::st_area(geometry)) |>
  dplyr::arrange(desc(area)) |>
  dplyr::slice(1)

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
  terra::crop(terra::vect(peninsula_only), mask = TRUE)

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
      partition == 8 ~ 22,
      partition == 9 ~ 23,
      partition %in% c(25, 26) ~ 40,
      partition == 59 ~ 60,
      partition == 70 ~ 56,
      partition == 83 ~ 82,
      partition == 96 ~ 95,
      partition %in% c(100, 114) ~ 115,
      partition == 123 ~ 122,
      partition == 128 ~ 129,
      partition == 142 ~ 143,
      partition == 178 ~ 164,
      partition == 199 ~ 200,
      .default = partition
    )
  )
topo_arranged$partition |> unique() |> length()

sf::st_write(
  topo_arranged, "data-raw/peninsula_topo_500.gpkg",
  append = FALSE
)

# topo platon raster specs
raster_platon_specs <- list(
  extent = terra::ext(topo_spain) |> as.vector(),
  resolution = terra::res(topo_spain),
  crs = terra::crs(topo_spain)
)
saveRDS(raster_platon_specs, "data-raw/peninsula_platon_specs.rds")

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
    aes(color = topo_nrow, fill = bbox_area)
  ) +
  geom_sf_label(aes(label = i_step))

ggsave("data-raw/foo.png", foo)
