# # library(duckdb)
# # library(withr)
# # library(bench)
# # library(glue)
# # library(arrow)
# # library(geoarrow)
# # library(future)


# # duckdb_proxy <- dbConnect(duckdb())
# # withr::defer(dbDisconnect(duckdb_proxy))


# # # parquet_path <- "/srv/emf_data/fileserver/parquet/daily_interpolated_meteo/year=2024/month=1/day=1/*"
# # # # parquet_path <- "/srv/emf_data/fileserver/parquet/daily_interpolated_meteo/*/*/*/*"

# # # metadata_geomx_query <- glue::glue_sql(
# # #   .con = duckdb_proxy,
# # #   "SELECT file_name, row_group_id, stats_min AS xmin, stats_max AS xmax
# # #   FROM parquet_metadata({parquet_path})
# # #   WHERE path_in_schema = 'geom, x';"
# # # )

# # # metadata_geomy_query <- glue::glue_sql(
# # #   .con = duckdb_proxy,
# # #   "SELECT file_name, row_group_id, stats_min AS ymin, stats_max AS ymax
# # #   FROM parquet_metadata({parquet_path})
# # #   WHERE path_in_schema = 'geom, y';"
# # # )

# # # row_groups_bboxes <- dbGetQuery(duckdb_proxy, metadata_geomx_query) |>
# # #   dplyr::full_join(
# # #     dbGetQuery(duckdb_proxy, metadata_geomy_query),
# # #     by = c("row_group_id", "file_name")
# # #   ) |>
# # #   dplyr::mutate(
# # #     xmin = as.numeric(xmin), xmax = as.numeric(xmax),
# # #     ymin = as.numeric(ymin), ymax = as.numeric(ymax),
# # #     bbox = purrr::pmap(
# # #       list(xmin, xmax, ymax, ymin),
# # #       \(xmin, xmax, ymax, ymin) {
# # #         sf::st_bbox(
# # #           c(xmin = xmin, xmax = xmax, ymax = ymax, ymin = ymin),
# # #           crs = sf::st_crs(3043)
# # #         ) |>
# # #           sf::st_as_sfc()
# # #       }
# # #     ) |>
# # #       purrr::flatten()
# # #   ) |>
# # #   sf::st_as_sf(sf_column_name = "bbox")


# # # foo <- open_dataset(parquet_path) |>
# # #   sf::st_as_sf() |>
# # #   sf::st_transform(crs = 4326) |>
# # #   dplyr::mutate(
# # #     geohash_2 = geohashTools::gh_encode(sf::st_coordinates(geom)[,1], sf::st_coordinates(geom)[,2], precision = 2),
# # #     geohash_3 = geohashTools::gh_encode(sf::st_coordinates(geom)[,1], sf::st_coordinates(geom)[,2], precision = 3),
# # #     geohash_4 = geohashTools::gh_encode(sf::st_coordinates(geom)[,1], sf::st_coordinates(geom)[,2], precision = 4),
# # #     geohash_5 = geohashTools::gh_encode(sf::st_coordinates(geom)[,1], sf::st_coordinates(geom)[,2], precision = 5),
# # #     geohash_6 = geohashTools::gh_encode(sf::st_coordinates(geom)[,1], sf::st_coordinates(geom)[,2], precision = 6)
# # #   )
# # # foo |>
# # #   dplyr::select(geom, geohash_3) |>
# # #   dplyr::sample_frac(0.01) |>
# # #   plot()

# # #############################################################
# # # effect of row group size
# # parquet_path <- "/srv/emf_data/fileserver/parquet/daily_interpolated_meteo"
# # parquet_test <- open_dataset(parquet_path)

# # c(
# #   "40000", "60000", "50000", "300000", "400000"
# # ) |>
# #   purrr::map(
# #     \(rg) {
# #       if (as.numeric(rg) < 30000) {
# #         write_dataset(
# #           parquet_test,
# #           path = glue::glue("data-raw/parquet_rg_{rg}"),
# #           format = "parquet",
# #           partitioning = c("year", "month", "day"),
# #           existing_data_behavior = "overwrite",
# #           max_rows_per_group = as.numeric(rg)
# #         )
# #       } else {
# #         write_dataset(
# #           parquet_test,
# #           path = glue::glue("data-raw/parquet_rg_{rg}"),
# #           format = "parquet",
# #           partitioning = c("year", "month", "day"),
# #           existing_data_behavior = "overwrite",
# #           min_rows_per_group = as.numeric(rg)
# #         )
# #       }
# #     }
# #   )

# # # metadata_query <- glue::glue(
# # #   # .con = duckdb_proxy,
# # #   "SELECT * 
# # #   FROM parquet_metadata('data-raw/parquet_rg_{rg}/*/*/*/*')
# # #   LIMIT 1
# # #   ;"
# # # )
# # # purrr::map(metadata_query, ~ dbGetQuery(duckdb_proxy, .x))

# # #############################################################
# # test_bbox <- sf::st_read("data-raw/nfi_points.gpkg") |>
# #   dplyr::sample_n(100) |>
# #   sf::st_transform(crs = 3043) |>
# #   dplyr::mutate(
# #     envelope = sf::st_buffer(geom, 141.43),
# #     envelope = purrr::map(envelope, sf::st_bbox)
# #   ) |>
# #   dplyr::pull(envelope)

# # spatial_query <- glue::glue_sql(
# #   .con = duckdb_proxy,
# #   "SELECT 
# #     dates,
# #     avg(COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal'))
# #   FROM read_parquet('/srv/emf_data/fileserver/parquet/daily_interpolated_meteo/*/*/*/*')
# #   WHERE geom.x > {purrr::map_dbl(test_bbox, 'xmin')} AND
# #     geom.x < {purrr::map_dbl(test_bbox, 'xmax')} AND
# #     geom.y > {purrr::map_dbl(test_bbox, 'ymin')} AND
# #     geom.y < {purrr::map_dbl(test_bbox, 'ymax')}
# #   GROUP BY dates    
# #   ;"
# # )
# # spatial_query_40000 <- glue::glue_sql(
# #   .con = duckdb_proxy,
# #   "SELECT 
# #     dates,
# #     avg(COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal'))
# #   FROM read_parquet('data-raw/parquet_rg_40000/*/*/*/*')
# #   WHERE geom.x > {purrr::map_dbl(test_bbox, 'xmin')} AND
# #     geom.x < {purrr::map_dbl(test_bbox, 'xmax')} AND
# #     geom.y > {purrr::map_dbl(test_bbox, 'ymin')} AND
# #     geom.y < {purrr::map_dbl(test_bbox, 'ymax')}
# #   GROUP BY dates    
# #   ;"
# # )
# # spatial_query_60000 <- glue::glue_sql(
# #   .con = duckdb_proxy,
# #   "SELECT 
# #     dates,
# #     avg(COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal'))
# #   FROM read_parquet('data-raw/parquet_rg_60000/*/*/*/*')
# #   WHERE geom.x > {purrr::map_dbl(test_bbox, 'xmin')} AND
# #     geom.x < {purrr::map_dbl(test_bbox, 'xmax')} AND
# #     geom.y > {purrr::map_dbl(test_bbox, 'ymin')} AND
# #     geom.y < {purrr::map_dbl(test_bbox, 'ymax')}
# #   GROUP BY dates    
# #   ;"
# # )
# # spatial_query_50000 <- glue::glue_sql(
# #   .con = duckdb_proxy,
# #   "SELECT 
# #     dates,
# #     avg(COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal'))
# #   FROM read_parquet('data-raw/parquet_rg_50000/*/*/*/*')
# #   WHERE geom.x > {purrr::map_dbl(test_bbox, 'xmin')} AND
# #     geom.x < {purrr::map_dbl(test_bbox, 'xmax')} AND
# #     geom.y > {purrr::map_dbl(test_bbox, 'ymin')} AND
# #     geom.y < {purrr::map_dbl(test_bbox, 'ymax')}
# #   GROUP BY dates    
# #   ;"
# # )
# # spatial_query_300000 <- glue::glue_sql(
# #   .con = duckdb_proxy,
# #   "SELECT 
# #     dates,
# #     avg(COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal'))
# #   FROM read_parquet('data-raw/parquet_rg_300000/*/*/*/*')
# #   WHERE geom.x > {purrr::map_dbl(test_bbox, 'xmin')} AND
# #     geom.x < {purrr::map_dbl(test_bbox, 'xmax')} AND
# #     geom.y > {purrr::map_dbl(test_bbox, 'ymin')} AND
# #     geom.y < {purrr::map_dbl(test_bbox, 'ymax')}
# #   GROUP BY dates    
# #   ;"
# # )
# # spatial_query_400000 <- glue::glue_sql(
# #   .con = duckdb_proxy,
# #   "SELECT 
# #     dates,
# #     avg(COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal'))
# #   FROM read_parquet('data-raw/parquet_rg_400000/*/*/*/*')
# #   WHERE geom.x > {purrr::map_dbl(test_bbox, 'xmin')} AND
# #     geom.x < {purrr::map_dbl(test_bbox, 'xmax')} AND
# #     geom.y > {purrr::map_dbl(test_bbox, 'ymin')} AND
# #     geom.y < {purrr::map_dbl(test_bbox, 'ymax')}
# #   GROUP BY dates    
# #   ;"
# # )

# # bind_queries <- function(conn, queries) {
# #   purrr::map(
# #     queries,
# #     \(query) {
# #       dbGetQuery(conn, query)
# #     }
# #   ) |>
# #     purrr::list_rbind()
# # }

# # duckdb_benchmark <- bench::mark(
# #   parquet_default = bind_queries(duckdb_proxy, spatial_query),
# #   # parquet_40000 = bind_queries(duckdb_proxy, spatial_query_40000),
# #   # parquet_60000 = bind_queries(duckdb_proxy, spatial_query_60000),
# #   # parquet_50000 = bind_queries(duckdb_proxy, spatial_query_50000),
# #   # parquet_300000 = bind_queries(duckdb_proxy, spatial_query_300000),
# #   # parquet_400000 = bind_queries(duckdb_proxy, spatial_query_400000),
# #   parquet_default_50 = bind_queries(duckdb_proxy, spatial_query[1:50]),
# #   # parquet_40000_50 = bind_queries(duckdb_proxy, spatial_query_40000[1:50]),
# #   # parquet_60000_50 = bind_queries(duckdb_proxy, spatial_query_60000[1:50]),
# #   # parquet_50000_50 = bind_queries(duckdb_proxy, spatial_query_50000[1:50]),
# #   # parquet_300000_50 = bind_queries(duckdb_proxy, spatial_query_300000[1:50]),
# #   # parquet_400000_50 = bind_queries(duckdb_proxy, spatial_query_400000[1:50]),
# #   parquet_default_20 = bind_queries(duckdb_proxy, spatial_query[1:20]),
# #   # parquet_40000_20 = bind_queries(duckdb_proxy, spatial_query_40000[1:20]),
# #   # parquet_60000_20 = bind_queries(duckdb_proxy, spatial_query_60000[1:20]),
# #   # parquet_50000_20 = bind_queries(duckdb_proxy, spatial_query_50000[1:20]),
# #   # parquet_300000_20 = bind_queries(duckdb_proxy, spatial_query_300000[1:20]),
# #   # parquet_400000_20 = bind_queries(duckdb_proxy, spatial_query_400000[1:20]),
# #   iterations = 5, check = FALSE
# # )

# # ## query factory (OR)
# # # or_factory <- glue::glue(
# # #   "(geom.x > {purrr::map_dbl(test_bbox, 'xmin')[1:5]} AND
# # #     geom.x < {purrr::map_dbl(test_bbox, 'xmax')[1:5]} AND
# # #     geom.y > {purrr::map_dbl(test_bbox, 'ymin')[1:5]} AND
# # #     geom.y < {purrr::map_dbl(test_bbox, 'ymax')[1:5]})"
# # # ) |>
# # #   glue::glue_collapse(sep = " OR ", last = " OR ")

# # # or_spatial_query <- glue::glue(
# # #   # .con = duckdb_proxy,
# # #   "SELECT 
# # #     dates,
# # #     avg(COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal'))
# # #   FROM read_parquet('/srv/emf_data/fileserver/parquet/daily_interpolated_meteo/*/*/*/*')
# # #   WHERE {or_factory}
# # #   GROUP BY dates    
# # #   ;"
# # # )

# # # or_spatial_query_50000 <- glue::glue(
# # #   # .con = duckdb_proxy,
# # #   "SELECT 
# # #     dates,
# # #     avg(COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal'))
# # #   FROM read_parquet('data-raw/parquet_rg_50000/*/*/*/*')
# # #   WHERE {or_factory}
# # #   GROUP BY dates    
# # #   ;"
# # # )

# # # or_benchmark <- bench::mark(
# # #   # parquet_default = bind_queries(duckdb_proxy, spatial_query[1:5]),
# # #   # parquet_default_or = dbGetQuery(duckdb_proxy, or_spatial_query),
# # #   # parquet_40000 = bind_queries(duckdb_proxy, spatial_query_40000[1:5]),
# # #   # parquet_60000 = bind_queries(duckdb_proxy, spatial_query_60000[1:5]),
# # #   parquet_50000 = bind_queries(duckdb_proxy, spatial_query_50000[1:5]),
# # #   parquet_50000_or = dbGetQuery(duckdb_proxy, or_spatial_query_50000),
# # #   # parquet_300000 = bind_queries(duckdb_proxy, spatial_query_300000[1:5]),
# # #   # parquet_400000 = bind_queries(duckdb_proxy, spatial_query_400000[1:5]),
# # #   iterations = 1, check = FALSE
# # # )




# library(arrow)
# library(geoarrow)
# library(sf)
# library(dplyr)
# library(duckdb)
# library(withr)
# library(glue)


# # dataset directly over https not working
# # interpolated_meteo_path <- "https://data-emf.creaf.cat/public/parquet/daily_interpolated_meteo/"
# # parquet_test <- open_dataset(interpolated_meteo_path) # error

# # single file over https download the whole file, not ideal
# # foo <- "https://data-emf.creaf.cat/public/parquet/daily_interpolated_meteo/year=2024/month=2/day=27/part-0.parquet"
# # foo_test <- read_parquet(foo)

# ## duckdb httpsfs layer
# duckdb_proxy <- dbConnect(duckdb())
# withr::defer(dbDisconnect(duckdb_proxy))

# interpolated_meteo_path <-
#   "/srv/emf_data/fileserver/parquet/daily_interpolated_meteo/"

# install_httpfs_statement <- glue_sql(
#   .con = duckdb_proxy,
#   "INSTALL httpfs;"
# )
# httpfs_statement <- glue_sql(
#   .con = duckdb_proxy,
#   "LOAD httpfs;"
# )
# install_spatial_statement <- glue_sql(
#   .con = duckdb_proxy,
#   "INSTALL spatial;"
# )
# spatial_statement <- glue_sql(
#   .con = duckdb_proxy,
#   "LOAD spatial;"
# )

# dbExecute(duckdb_proxy, install_httpfs_statement)
# dbExecute(duckdb_proxy, httpfs_statement)
# dbExecute(duckdb_proxy, install_spatial_statement)
# dbExecute(duckdb_proxy, spatial_statement)

# parquet_files_vector <- seq(Sys.Date() - 370, Sys.Date() - 6, by = "day") |>
#   purrr::map_chr(
#     .f = \(i_date) {
#       glue::glue("/srv/emf_data/fileserver/parquet/daily_interpolated_meteo/year={lubridate::year(i_date)}/month={lubridate::month(i_date)}/day={lubridate::day(i_date)}/part-0.parquet")
#     }
#   )

# # test_bbox <- sf::st_read("~/Downloads/nfi_points.gpkg") |>
# #   dplyr::sample_n(100) |>
# #   sf::st_transform(crs = 25830) |>
# #   dplyr::mutate(
# #     envelope = sf::st_buffer(geom, sqrt((250^2) + (250^2))),
# #     envelope = purrr::map(envelope, sf::st_bbox)
# #   ) |>
# #   dplyr::pull(envelope)
# test_bbox <- list(c(xmin = 863973.805546189, ymin = 4694650.26881026, xmax = 864680.912327376, ymax = 4695357.37559145))

# parquet_files_array <- glue::glue(
#   '[{glue::glue_sql(.con = duckdb_proxy, "{parquet_files_vector}") |> glue::glue_sql_collapse(sep = ", ")}]'
# )

# spatial_query <- glue::glue(
#   # .con = duckdb_proxy,
#   "SELECT 
#     dates,
#     avg(COLUMNS('elevation|Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal')),
#     first(geom_text) AS geom_text
#   FROM read_parquet({parquet_files_array})
#   WHERE geom.x > {purrr::map_dbl(test_bbox, 'xmin')} AND
#     geom.x < {purrr::map_dbl(test_bbox, 'xmax')} AND
#     geom.y > {purrr::map_dbl(test_bbox, 'ymin')} AND
#     geom.y < {purrr::map_dbl(test_bbox, 'ymax')}
#   GROUP BY dates    
#   ;"
# )

# tictoc::tic()
# spatial_query_res <- dbGetQuery(duckdb_proxy, spatial_query)
# tictoc::toc()

# # direct access to files
# open_dataset(interpolated_meteo_path) |>
#   dplyr::filter(sf::st_intersects(test_bbox, geom))

library(arrow)

# foo <- s3_bucket(
#   "s3://meteoland-spain-app-bucket",
#   access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
#   secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
#   scheme = "https",
#   endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
#   region = ""
# )


foo <- S3FileSystem$create(
  access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
  secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
  scheme = "https",
  endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
  region = ""
)

meteoland_s3 <- foo$cd("meteoland-spain-app-bucket")

pngs_s3 <- foo$cd("meteoland-spain-app-pngs")

bar <- open_dataset(pngs_s3) |>
  dplyr::filter(date == "20250501") |>
  dplyr::as_tibble()
