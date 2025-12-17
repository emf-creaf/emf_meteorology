calculate_daily_averages <- function(parquet_files, admin_level) {
  stopifnot(!is.null(parquet_files))
  # get the selected admin polygons, removing those belonging to Canarias,
  # Ceuta, Melilla
  admin_polygons <- switch(
    admin_level,
    "provincia" = mapSpain::esp_get_prov_siane(epsg = 4326, resolution = 10) |>
      dplyr::filter(
        !ine.prov.name %in%
          c("Ceuta", "Melilla", "Palmas, Las", "Santa Cruz de Tenerife"),
        !is.na(cpro)
      ) |>
      dplyr::select(name = ine.prov.name, geom) |>
      sf::st_transform(crs = 25830),
    "municipio" = mapSpain::esp_get_munic_siane(epsg = 4326, resolution = 10) |>
      dplyr::filter(
        !ine.prov.name %in%
          c("Ceuta", "Melilla", "Palmas, Las", "Santa Cruz de Tenerife"),
        !is.na(cpro),
        !is.na(name)
      ) |>
      dplyr::select(name, geom) |>
      sf::st_transform(crs = 25830),
    "comarca" = mapSpain::esp_get_comarca(epsg = 4326, type = "INE") |>
      dplyr::filter(
        !ine.prov.name %in%
          c("Ceuta", "Melilla", "Palmas, Las", "Santa Cruz de Tenerife"),
        !is.na(cpro)
      ) |>
      dplyr::select(name, geom) |>
      sf::st_transform(crs = 25830)
  )

  mirai::daemons(6)
  withr::defer(mirai::daemons(0))
  mirai::everywhere({
    # db preparation
    duckdb_proxy <<- DBI::dbConnect(duckdb::duckdb())
    # withr::defer(DBI::dbDisconnect(duckdb_proxy))
    install_httpfs_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "INSTALL httpfs;"
    )
    httpfs_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "LOAD httpfs;"
    )
    install_spatial_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "INSTALL spatial;"
    )
    spatial_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "LOAD spatial;"
    )
    credentials_statement <- glue::glue(
      "CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{Sys.getenv('AWS_ACCESS_KEY_ID')}',
        SECRET '{Sys.getenv('AWS_SECRET_ACCESS_KEY')}',
        REGION '',
        ENDPOINT '{Sys.getenv('AWS_S3_ENDPOINT')}'
      );"
    )
    DBI::dbExecute(duckdb_proxy, install_httpfs_statement)
    DBI::dbExecute(duckdb_proxy, httpfs_statement)
    DBI::dbExecute(duckdb_proxy, install_spatial_statement)
    DBI::dbExecute(duckdb_proxy, spatial_statement)
    DBI::dbExecute(duckdb_proxy, credentials_statement)
  })

  res <- mirai::mirai_map(
    admin_polygons,
    \(name, geom) {
      # row query
      ts_query <- glue::glue("
        FROM (
          FROM
            read_parquet('{parquet_files}*.parquet', hive_partitioning=true)
          SELECT
            dates,
            ST_Point(geom.x, geom.y)::GEOMETRY AS geom,
            COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal')
        )
        SELECT
          dates,
          avg(MeanTemperature) FILTER (NOT isnan(MeanTemperature)) AS MeanTemperature,
          avg(MinTemperature) FILTER (NOT isnan(MinTemperature)) AS MinTemperature,
          avg(MaxTemperature) FILTER (NOT isnan(MaxTemperature)) AS MaxTemperature,
          avg(Precipitation) FILTER (NOT isnan(Precipitation)) AS Precipitation,
          avg(MeanRelativeHumidity) FILTER (NOT isnan(MeanRelativeHumidity)) AS MeanRelativeHumidity,
          avg(MinRelativeHumidity) FILTER (NOT isnan(MinRelativeHumidity)) AS MinRelativeHumidity,
          avg(MaxRelativeHumidity) FILTER (NOT isnan(MaxRelativeHumidity)) AS MaxRelativeHumidity,
          avg(Radiation) FILTER (NOT isnan(Radiation)) AS Radiation,
          avg(WindSpeed) FILTER (NOT isnan(WindSpeed)) AS WindSpeed,
          avg(WindDirection) FILTER (NOT isnan(WindDirection)) AS WindDirection,
          avg(PET) FILTER (NOT isnan(PET)) AS PET,
          avg(ThermalAmplitude) FILTER (NOT isnan(ThermalAmplitude)) AS ThermalAmplitude
        WHERE ST_Intersects(geom, ST_GeomFromText('{sf::st_as_text(geom)}'))
        GROUP BY dates
        ;
      ")
      DBI::dbGetQuery(duckdb_proxy, ts_query) |>
        dplyr::as_tibble() |>
        dplyr::mutate(
          name = name,
          geom = sf::st_as_text(geom)
        )
    },
    parquet_files = parquet_files
  )[]

  res <- res |>
    purrr::list_rbind() |>
    dplyr::mutate(admin_level = admin_level)

  mirai::everywhere({
    DBI::dbDisconnect(duckdb_proxy)
  })
  return(res)
}

write_meteoland_timeseries <- function(daily_averages) {
  # s3 filesystem
  s3_fs <- S3FileSystem$create(
    access_key = Sys.getenv("AWS_ACCESS_KEY_ID"),
    secret_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    scheme = "https",
    endpoint_override = Sys.getenv("AWS_S3_ENDPOINT"),
    region = ""
  )

  data_to_write <- daily_averages |>
    purrr::list_rbind()

  file_name <- switch(
    unique(data_to_write$admin_level),
    "municipio" = "meteoland-spain-app-pngs/daily_interpolated_meteo_timeseries_municipio.parquet",
    "comarca" = "meteoland-spain-app-pngs/daily_interpolated_meteo_timeseries_comarca.parquet",
    "provincia" = "meteoland-spain-app-pngs/daily_interpolated_meteo_timeseries_provincia.parquet"
  )

  # write png tibble
  daily_averages |>
    purrr::list_rbind() |>
    arrow::write_parquet(
      sink = s3_fs$path(file_name)
    )

  return(paste0("s3://", file_name))
}

calculate_daily_averages_for_munis <- function(parquet_files) {
  # browser()
  # get the selected admin polygons, removing those belonging to Canarias,
  # Ceuta, Melilla
  admin_polygons <-
    mapSpain::esp_get_munic_siane(epsg = 4326, resolution = 10) |>
    dplyr::filter(
      !ine.prov.name %in%
        c("Ceuta", "Melilla", "Palmas, Las", "Santa Cruz de Tenerife"),
      !is.na(cpro),
      !is.na(name)
    ) |>
    dplyr::select(name, geom) |>
    sf::st_transform(crs = 25830)

  mirai::daemons(6, output = TRUE)
  withr::defer({
    mirai::everywhere({DBI::dbDisconnect(duckdb_proxy)})
    mirai::daemons(0)
  })
  mirai::everywhere({
    # db preparation
    duckdb_proxy <<- DBI::dbConnect(duckdb::duckdb())
    # withr::defer(DBI::dbDisconnect(duckdb_proxy))
    install_httpfs_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "INSTALL httpfs;"
    )
    httpfs_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "LOAD httpfs;"
    )
    install_spatial_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "INSTALL spatial;"
    )
    spatial_statement <- glue::glue_sql(
      .con = duckdb_proxy,
      "LOAD spatial;"
    )
    credentials_statement <- glue::glue(
      "CREATE OR REPLACE SECRET secret (
        TYPE s3,
        PROVIDER config,
        KEY_ID '{Sys.getenv('AWS_ACCESS_KEY_ID')}',
        SECRET '{Sys.getenv('AWS_SECRET_ACCESS_KEY')}',
        REGION '',
        ENDPOINT '{Sys.getenv('AWS_S3_ENDPOINT')}'
      );"
    )
    DBI::dbExecute(duckdb_proxy, install_httpfs_statement)
    DBI::dbExecute(duckdb_proxy, httpfs_statement)
    DBI::dbExecute(duckdb_proxy, install_spatial_statement)
    DBI::dbExecute(duckdb_proxy, spatial_statement)
    DBI::dbExecute(duckdb_proxy, credentials_statement)
  })

  step_rows <- 500
  splitted_polys <- split(
    admin_polygons,
    ceiling(seq_along(1:nrow(admin_polygons)) / step_rows)
  )

  lapply(
    splitted_polys,
    \(admin_polygons_chunk) {
      res <- mirai::mirai_map(
        admin_polygons_chunk,
        \(name, geom) {
          # row query
          ts_query <- glue::glue("
            FROM (
              FROM
                read_parquet('{parquet_files}*.parquet', hive_partitioning=true)
              SELECT
                dates,
                ST_Point(geom.x, geom.y)::GEOMETRY AS geom,
                COLUMNS('Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal')
            )
            SELECT
              dates,
              avg(MeanTemperature) FILTER (NOT isnan(MeanTemperature)) AS MeanTemperature,
              avg(MinTemperature) FILTER (NOT isnan(MinTemperature)) AS MinTemperature,
              avg(MaxTemperature) FILTER (NOT isnan(MaxTemperature)) AS MaxTemperature,
              avg(Precipitation) FILTER (NOT isnan(Precipitation)) AS Precipitation,
              avg(MeanRelativeHumidity) FILTER (NOT isnan(MeanRelativeHumidity)) AS MeanRelativeHumidity,
              avg(MinRelativeHumidity) FILTER (NOT isnan(MinRelativeHumidity)) AS MinRelativeHumidity,
              avg(MaxRelativeHumidity) FILTER (NOT isnan(MaxRelativeHumidity)) AS MaxRelativeHumidity,
              avg(Radiation) FILTER (NOT isnan(Radiation)) AS Radiation,
              avg(WindSpeed) FILTER (NOT isnan(WindSpeed)) AS WindSpeed,
              avg(WindDirection) FILTER (NOT isnan(WindDirection)) AS WindDirection,
              avg(PET) FILTER (NOT isnan(PET)) AS PET,
              avg(ThermalAmplitude) FILTER (NOT isnan(ThermalAmplitude)) AS ThermalAmplitude
            WHERE ST_Intersects(geom, ST_GeomFromText('{sf::st_as_text(geom)}'))
            GROUP BY dates
            ;
          ")
          DBI::dbGetQuery(duckdb_proxy, ts_query) |>
            dplyr::as_tibble() |>
            dplyr::mutate(
              name = name,
              geom = sf::st_as_text(geom)
            )
        },
        parquet_files = parquet_files
      )[]

      res <- res |>
        purrr::list_rbind() |>
        dplyr::mutate(admin_level = admin_level)

      return(res)
    }
  ) |>
    purrr::list_rbind()
}