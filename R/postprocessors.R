cross_validations_postprocessor <- function(cross_validation) {
  purrr::imap(
    cross_validation, .f = \(cv, i_step) {
      interpolator_id <- stringr::str_extract(i_step, "_\\d+\\.") |>
        stringr::str_remove_all("_|\\.")

      cv$dates_stats[nrow(cv$dates_stats), ] |>
        tidyr::pivot_longer(!dates, names_to = "variable_date_stat") |>
        tidyr::separate_wider_regex(
          variable_date_stat,
          c(variable = "^.*", "_date_", stat = ".*$")
        ) |>
        dplyr::filter(stat %in% c("bias", "mae", "relative_bias")) |>
        dplyr::bind_rows(
          dplyr::tibble(
            dates = cv$dates_stats[["dates"]][nrow(cv$dates_stats)],
            variable = names(cv$r2),
            stat = "r2",
            value = purrr::flatten_dbl(cv$r2)
          )
        ) |>
        dplyr::mutate(interpolator_id = interpolator_id)
    }
  ) |>
    purrr::list_rbind()
}

cross_validation_writer <- function(cv_tables) {
  cv_path <-
    "/srv/emf_data/fileserver/parquet/cross_validations/daily_interpolated_meteo_cv.parquet"
  cv_tables |>
    purrr::list_rbind() |>
    arrow::write_parquet(sink = cv_path)

  return(cv_path)
}