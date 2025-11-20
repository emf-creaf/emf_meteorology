calculate_daily_averages <- function(parquet_files, admin_level) {
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
        !is.na(cpro)
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


  mirai::daemons(5)
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
    \(name, cpro, geom) {
      # row query
      ts_query <- glue::glue("
        FROM (
          FROM
            read_parquet('{parquet_files}*.parquet', hive_partitioning=true)
          SELECT
            dates,
            ST_Point(geom.x, geom.y)::GEOMETRY AS geom,
            COLUMNS('elevation|slope|aspect|Temperature|Prec|Humidity|Radiation|Wind|PET|Thermal')
        )
        SELECT
          dates,
          avg(elevation) FILTER (NOT isnan(elevation)) AS elevation,
          avg(slope) FILTER (NOT isnan(slope)) AS slope,
          avg(aspect) FILTER (NOT isnan(aspect)) AS aspect,
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
          comarca = name,
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

  # write png tibble
  daily_averages |>
    purrr::list_rbind() |>
    arrow::write_parquet(
      sink = s3_fs$path("meteoland-spain-app-pngs/daily_interpolated_meteo_timeseries.parquet")
    )

  return("s3://meteoland-spain-app-pngs/daily_interpolated_meteo_timeseries.parquet")
}