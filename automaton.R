## libraries
library(targets)
library(ntfy)

## check working directory
setwd(Sys.getenv("PIPELINE_PATH"))
cli::cli_alert_info("Current active pipeline: {.file {getwd()}}")

## daily make
tar_make(reporter = "timestamp_positives")

## daily prune
if (length(tar_prune_list() > 0)) {
  cli::cli_inform(c(
    "!" = "Pruning old targets:",
    tar_prune_list()
  ))
  tar_prune()
}

## notify
pipeline_log_summary <- tar_progress_summary(fields = NULL)
ntfy_tag <- ifelse(pipeline_log_summary$errored > 0, c(tags$lady_beetle), c(tags$bell))

ntfy_send(
  message = glue::glue(
    "emf_meteorology pipeline ({pipeline_log_summary$time}):    ",
    "skipped: {pipeline_log_summary$skipped} - ",
    "completed: {pipeline_log_summary$completed} - ",
    "errored: {pipeline_log_summary$errored}"
  ),
  tags = ntfy_tag,
  priority = 2,
  topic = "emf_pipelines"
)
