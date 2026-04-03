ensure_required_packages <- function(packages, repos = "https://cloud.r-project.org") {
  minor_major <- strsplit(R.version$minor, "\\.")[[1]][1]
  version_short <- paste(R.version$major, minor_major, sep = ".")
  local_app_data <- Sys.getenv("LOCALAPPDATA")
  if (!nzchar(local_app_data)) {
    local_app_data <- path.expand("~")
  }
  user_lib <- file.path(local_app_data, "R", "win-library", version_short)
  if (!dir.exists(user_lib)) {
    dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
  }
  if (!(user_lib %in% .libPaths())) {
    .libPaths(c(user_lib, .libPaths()))
  }

  installed <- rownames(installed.packages())
  missing <- setdiff(packages, installed)

  if (length(missing) > 0) {
    cat(sprintf(
      "Missing R packages: %s\nAutomatic installation in progress...\n",
      paste(missing, collapse = ", ")
    ))

    tryCatch(
      {
        install.packages(missing, repos = repos, lib = user_lib)
      },
      error = function(e) {
        stop(sprintf(
          "Automatic installation failed (%s). Please manually install the missing packages and try again.",
          conditionMessage(e)
        ))
      }
    )
  }

  still_missing <- setdiff(packages, rownames(installed.packages()))
  if (length(still_missing) > 0) {
    stop(sprintf(
      "Packages not available after installation: %s",
      paste(still_missing, collapse = ", ")
    ))
  }
}

ensure_required_packages(c("DBI", "RSQLite", "ggplot2", "gridExtra"))

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(ggplot2)
  library(gridExtra)
  library(grid)
})

`%||%` <- function(a, b) {
  if (is.null(a)) b else a
}

CATEGORY_ORDER <- c("No-change", "Initial", "Improving", "Worsening")
CATEGORY_COLORS <- c(
  "No-change" = "#F8766D",
  "Initial" = "#7CAE00",
  "Improving" = "#00BFC4",
  "Worsening" = "#C77CFF"
)
SECONDS_PER_DAY <- 86400

get_script_dir <- function() {
  file_arg <- commandArgs(trailingOnly = FALSE)
  file_match <- grep("^--file=", file_arg, value = TRUE)
  if (length(file_match) > 0) {
    script_path <- sub("^--file=", "", file_match[[1]])
    return(normalizePath(dirname(script_path), winslash = "/", mustWork = FALSE))
  }
  normalizePath(getwd(), winslash = "/", mustWork = FALSE)
}

SCRIPT_DIR <- get_script_dir()
PROJECT_ROOT <- normalizePath(file.path(SCRIPT_DIR, ".."), winslash = "/", mustWork = FALSE)

resolve_input_path <- function(path_value) {
  if (grepl("^[A-Za-z]:[/\\\\]", path_value) || startsWith(path_value, "/")) {
    return(normalizePath(path_value, winslash = "/", mustWork = FALSE))
  }

  candidates <- c(
    file.path(PROJECT_ROOT, path_value),
    file.path(SCRIPT_DIR, path_value),
    file.path(getwd(), path_value)
  )

  for (candidate in candidates) {
    if (file.exists(candidate)) {
      return(normalizePath(candidate, winslash = "/", mustWork = FALSE))
    }
  }

  normalizePath(file.path(PROJECT_ROOT, path_value), winslash = "/", mustWork = FALSE)
}

parse_iso_datetime <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x[x == ""] <- NA_character_
  x[tolower(x) %in% c("na", "nan", "null", "none")] <- NA_character_

  # Normalize timezone
  x <- sub("Z$", "+0000", x)
  x <- sub("([+-][0-9]{2}):([0-9]{2})$", "\\1\\2", x)

  formats <- c(
    "%Y-%m-%dT%H:%M:%OS%z",
    "%Y-%m-%d %H:%M:%OS%z",
    "%Y-%m-%dT%H:%M:%OS",
    "%Y-%m-%d %H:%M:%OS",
    "%Y-%m-%d"
  )

  parse_one <- function(value) {
    if (is.na(value)) {
      return(as.POSIXct(NA, tz = "UTC"))
    }

    for (fmt in formats) {
      parsed <- suppressWarnings(as.POSIXct(value, tz = "UTC", format = fmt))
      if (!is.na(parsed)) {
        return(parsed)
      }
    }

    as.POSIXct(NA, tz = "UTC")
  }

  as.POSIXct(vapply(x, parse_one, FUN.VALUE = as.POSIXct(NA, tz = "UTC")), origin = "1970-01-01", tz = "UTC")
}

read_report_commits <- function(db_path) {
  db_resolved <- resolve_input_path(db_path)
  if (!file.exists(db_resolved)) {
    stop(sprintf("Database not found: %s", db_resolved))
  }

  con <- dbConnect(RSQLite::SQLite(), db_resolved)
  on.exit(dbDisconnect(con), add = TRUE)

  query <- "
    SELECT
      dataset,
      repository,
      file_name,
      commit_hash,
      date,
      nearest_future_release_date,
      nearest_previous_release_date,
      smells_count
    FROM report_commit_details
  "

  frame <- dbGetQuery(con, query)
  if (nrow(frame) == 0) {
    return(frame)
  }

  frame$date <- parse_iso_datetime(frame$date)
  frame$nearest_future_release_date <- parse_iso_datetime(frame$nearest_future_release_date)
  frame$nearest_previous_release_date <- parse_iso_datetime(frame$nearest_previous_release_date)
  frame$smells_count <- as.integer(ifelse(is.na(frame$smells_count), 0, frame$smells_count))
  frame <- frame[!is.na(frame$date), ]
  frame
}

closest_release_signed_days <- function(commit_date, prev_date, future_date) {
  candidates <- numeric(0)

  if (!is.na(prev_date)) {
    candidates <- c(candidates, as.numeric(difftime(commit_date, prev_date, units = "days")))
  }
  if (!is.na(future_date)) {
    candidates <- c(candidates, as.numeric(difftime(commit_date, future_date, units = "days")))
  }

  if (length(candidates) == 0) {
    return(NA_real_)
  }

  candidates[which.min(abs(candidates))]
}

classify_variation <- function(df) {
  if (nrow(df) == 0) return(df)

  # Sort by date and commit_hash (as in Python)
  o <- order(df$date, df$commit_hash)
  ordered <- df[o, ]

  # Shift smells_count down by 1, fill 0 (like fillna(0) in Python)
  prev <- c(0L, head(ordered$smells_count, -1))
  delta <- ordered$smells_count - prev

  variation <- ifelse(
    delta < 0,
    "Improving",
    ifelse(delta > 0, ifelse(prev == 0, "Initial", "Worsening"), "No-change")
  )

  ordered$variation_type <- variation
  ordered
}

build_plot_data <- function(frame) {
  if (nrow(frame) == 0) return(frame)

  split_key <- paste(frame$dataset, frame$repository, frame$file_name, sep = "||")
  groups <- split(frame, split_key)
  classified_list <- lapply(groups, classify_variation)
  classified <- do.call(rbind, classified_list)

  classified$signed_days <- mapply(
    closest_release_signed_days,
    classified$date,
    classified$nearest_previous_release_date,
    classified$nearest_future_release_date
  )

  classified <- classified[!is.na(classified$signed_days), ]
  classified <- classified[classified$signed_days >= -365 & classified$signed_days <= 365, ]
  classified$variation_type <- factor(classified$variation_type, levels = CATEGORY_ORDER)
  classified
}

plot_ridge_manual <- function(frame, output_png, figure_width, figure_height, bw_adjust, dpi) {
  if (nrow(frame) == 0) {
    stop("No valid data found to generate the plot.")
  }

  x_values <- frame$signed_days
  x_min <- floor(min(x_values, na.rm = TRUE) / 10) * 10 - 10
  x_max <- ceiling(max(x_values, na.rm = TRUE) / 10) * 10 + 10
  x_grid <- seq(x_min, x_max, length.out = 500)

  density_data <- data.frame()
  max_height <- 0.9

  for (i in seq_along(CATEGORY_ORDER)) {
    category <- CATEGORY_ORDER[i]
    baseline <- i - 1
    vals <- frame$signed_days[frame$variation_type == category]

    if (length(vals) == 0) {
      next
    }

    if (length(vals) == 1) {
      bandwidth <- 5.0 * max(bw_adjust, 0.1)
      y <- dnorm((x_grid - vals[[1]]) / bandwidth) / bandwidth
      dens <- list(x = x_grid, y = y)
    } else {
      dens <- density(vals, bw = "nrd0", adjust = max(bw_adjust, 0.1), n = 500, from = x_min, to = x_max)
    }
    y_scaled <- if (max(dens$y) > 0) (dens$y / max(dens$y)) * max_height else rep(0, length(dens$y))

    density_data <- rbind(
      density_data,
      data.frame(
        x = dens$x,
        y = baseline + y_scaled,
        baseline = baseline,
        variation_type = category,
        stringsAsFactors = FALSE
      )
    )
  }

  rug_data <- frame[, c("signed_days", "variation_type")]
  rug_data$ymin <- -0.18
  rug_data$ymax <- -0.04

  baselines_df <- data.frame(
    variation_type = CATEGORY_ORDER,
    baseline = seq_along(CATEGORY_ORDER) - 1
  )

  p <- ggplot() +
    geom_hline(data = baselines_df, aes(yintercept = baseline), color = "black", linewidth = 0.35) +
    geom_ribbon(
      data = density_data,
      aes(x = x, ymin = baseline, ymax = y, fill = variation_type),
      alpha = 0.6
    ) +
    geom_line(data = density_data, aes(x = x, y = y, group = variation_type), color = "black", linewidth = 0.55) +
    geom_segment(
      data = rug_data,
      aes(x = signed_days, xend = signed_days, y = ymin, yend = ymax, color = variation_type),
      linewidth = 0.5,
      alpha = 0.95
    ) +
    geom_vline(xintercept = 0, linetype = "dashed", linewidth = 0.5) +
    scale_fill_manual(values = CATEGORY_COLORS, breaks = CATEGORY_ORDER, drop = FALSE) +
    scale_color_manual(values = CATEGORY_COLORS, breaks = CATEGORY_ORDER, drop = FALSE) +
    scale_y_continuous(
      breaks = seq_along(CATEGORY_ORDER) - 1,
      labels = CATEGORY_ORDER,
      limits = c(-0.25, (length(CATEGORY_ORDER) - 1) + max_height + 0.05)
    ) +
    scale_x_continuous(limits = c(x_min, x_max)) +
    labs(
      x = "Days relative to closest release (negative = before, positive = after)",
      y = "Density"
    ) +
    theme_minimal(base_size = 12) +
    theme(
      panel.grid.major = element_line(color = "#ffffff", linewidth = 0.5),
      panel.grid.minor = element_line(color = "#ffffff", linewidth = 0.25),
      panel.background = element_rect(fill = "#EBEBEB", color = NA),
      plot.background = element_rect(fill = "white", color = NA),
      legend.position = "none"
    )

  output_dir <- dirname(output_png)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  ggsave(output_png, p, width = figure_width, height = figure_height, dpi = dpi)
}

print_counts <- function(plot_data, label) {
  counts <- table(factor(plot_data$variation_type, levels = CATEGORY_ORDER))
  cat(sprintf("Ridge plot created (%s): %s\n", label, if (label == "JS") output_js else output_ts))
  cat(sprintf("Observations by variation type (%s):\n", label))
  for (name in CATEGORY_ORDER) {
    cat(sprintf("- %s: %d\n", name, as.integer(counts[[name]])))
  }
}

build_release_proximity_summary <- function(plot_data) {
  if (nrow(plot_data) == 0) {
    stop("No data available to build release-cycle proximity summary.")
  }

  col_labels <- c("No-change", "Introd.", "Improv.", "Worsen.")
  thresholds <- c(7, 14, 30)

  values_by_category <- lapply(CATEGORY_ORDER, function(category) {
    abs(plot_data$signed_days[plot_data$variation_type == category])
  })
  names(values_by_category) <- col_labels

  format_int <- function(x) {
    if (is.na(x)) "NA" else sprintf("%d", as.integer(round(x)))
  }

  format_decimal <- function(x, digits = 1) {
    if (is.na(x)) "NA" else sprintf(paste0("%.", digits, "f"), x)
  }

  num_commits <- vapply(values_by_category, length, integer(1))

  within_rows <- lapply(thresholds, function(k) {
    vapply(values_by_category, function(v) {
      if (length(v) == 0) {
        return(NA_real_)
      }
      mean(v <= k) * 100
    }, numeric(1))
  })

  medians <- vapply(values_by_category, function(v) {
    if (length(v) == 0) {
      return(NA_real_)
    }
    as.numeric(median(v))
  }, numeric(1))

  iqrs <- vapply(values_by_category, function(v) {
    if (length(v) == 0) {
      return(NA_real_)
    }
    as.numeric(IQR(v))
  }, numeric(1))

  summary_table <- data.frame(
    Metric = c(
      "Num. of commits",
      "Within \u00B17 days (%)",
      "Within \u00B114 days (%)",
      "Within \u00B130 days (%)",
      "Median |d|",
      "IQR |d|"
    ),
    "No-change" = c(
      format_int(num_commits[["No-change"]]),
      format_decimal(within_rows[[1]][["No-change"]], 1),
      format_decimal(within_rows[[2]][["No-change"]], 1),
      format_decimal(within_rows[[3]][["No-change"]], 1),
      format_int(medians[["No-change"]]),
      format_decimal(iqrs[["No-change"]], 1)
    ),
    "Introd." = c(
      format_int(num_commits[["Introd."]]),
      format_decimal(within_rows[[1]][["Introd."]], 1),
      format_decimal(within_rows[[2]][["Introd."]], 1),
      format_decimal(within_rows[[3]][["Introd."]], 1),
      format_int(medians[["Introd."]]),
      format_decimal(iqrs[["Introd."]], 1)
    ),
    "Improv." = c(
      format_int(num_commits[["Improv."]]),
      format_decimal(within_rows[[1]][["Improv."]], 1),
      format_decimal(within_rows[[2]][["Improv."]], 1),
      format_decimal(within_rows[[3]][["Improv."]], 1),
      format_int(medians[["Improv."]]),
      format_decimal(iqrs[["Improv."]], 1)
    ),
    "Worsen." = c(
      format_int(num_commits[["Worsen."]]),
      format_decimal(within_rows[[1]][["Worsen."]], 1),
      format_decimal(within_rows[[2]][["Worsen."]], 1),
      format_decimal(within_rows[[3]][["Worsen."]], 1),
      format_int(medians[["Worsen."]]),
      format_decimal(iqrs[["Worsen."]], 1)
    ),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )

  summary_table
}

save_release_proximity_table_png <- function(summary_table, output_png, dataset_label) {
  output_dir <- dirname(output_png)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  labels <- names(summary_table)
  metric_values <- summary_table$Metric
  body_values <- summary_table[, c("No-change", "Introd.", "Improv.", "Worsen."), drop = FALSE]

  n_body <- nrow(summary_table)

  col_widths <- c(0.37, 0.1575, 0.1575, 0.1575, 0.1575)
  x_edges <- c(0, cumsum(col_widths))
  x_centers <- head(x_edges, -1) + col_widths / 2

  top_margin <- 0.03
  bottom_margin <- 0.06
  header1_h <- 0.15
  header2_h <- 0.12
  body_h <- (1 - top_margin - bottom_margin - header1_h - header2_h) / max(n_body, 1)

  y_top <- 1 - top_margin
  y_header1_bottom <- y_top - header1_h
  y_header2_bottom <- y_header1_bottom - header2_h

  png(output_png, width = 1700, height = 750, res = 170)
  grid.newpage()

  grid.rect(
    x = unit(0.5, "npc"),
    y = unit(0.5, "npc"),
    width = unit(1, "npc"),
    height = unit(1, "npc"),
    gp = gpar(fill = "#d9d9d9", col = NA)
  )

  grid.lines(
    x = unit(c(0.01, 0.99), "npc"),
    y = unit(c(y_top, y_top), "npc"),
    gp = gpar(col = "black", lwd = 3)
  )

  grid.lines(
    x = unit(c(x_edges[2], x_edges[6]), "npc"),
    y = unit(c(y_header1_bottom, y_header1_bottom), "npc"),
    gp = gpar(col = "#666666", lwd = 1.6)
  )

  grid.lines(
    x = unit(c(0.01, 0.99), "npc"),
    y = unit(c(y_header2_bottom, y_header2_bottom), "npc"),
    gp = gpar(col = "black", lwd = 1.7)
  )

  y_bottom <- y_header2_bottom - n_body * body_h - 0.02
  grid.lines(
    x = unit(c(0.01, 0.99), "npc"),
    y = unit(c(y_bottom, y_bottom), "npc"),
    gp = gpar(col = "black", lwd = 3)
  )

  grid.text(
    "Variation type",
    x = unit((x_edges[2] + x_edges[6]) / 2, "npc"),
    y = unit((y_top + y_header1_bottom) / 2, "npc"),
    gp = gpar(fontfamily = "serif", fontface = "bold", cex = 2.0)
  )

  grid.text(
    labels[[1]],
    x = unit(x_edges[1] + 0.02, "npc"),
    y = unit((y_header1_bottom + y_header2_bottom) / 2, "npc"),
    just = c("left", "center"),
    gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.9)
  )

  for (j in 2:5) {
    grid.text(
      labels[[j]],
      x = unit(x_centers[j], "npc"),
      y = unit((y_header1_bottom + y_header2_bottom) / 2, "npc"),
      gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.8)
    )
  }

  for (i in seq_len(n_body)) {
    y_center <- y_header2_bottom - (i - 0.5) * body_h
    grid.text(
      metric_values[[i]],
      x = unit(x_edges[1] + 0.02, "npc"),
      y = unit(y_center, "npc"),
      just = c("left", "center"),
      gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.7)
    )

    for (j in 1:4) {
      grid.text(
        as.character(body_values[i, j]),
        x = unit(x_centers[j + 1], "npc"),
        y = unit(y_center, "npc"),
        gp = gpar(fontfamily = "serif", cex = 1.7)
      )
    }
  }

  dev.off()
}

save_release_proximity_summary <- function(plot_data, dataset_label, project_root) {
  summary_table <- build_release_proximity_summary(plot_data)

  csv_output <- normalizePath(
    file.path(project_root, "analyses", "reports", sprintf("release_cycle_proximity_summary_%s_R.csv", tolower(dataset_label))),
    winslash = "/",
    mustWork = FALSE
  )
  png_output <- normalizePath(
    file.path(project_root, "analyses", "plots", sprintf("release_cycle_proximity_summary_%s_R.png", tolower(dataset_label))),
    winslash = "/",
    mustWork = FALSE
  )

  csv_dir <- dirname(csv_output)
  if (!dir.exists(csv_dir)) {
    dir.create(csv_dir, recursive = TRUE)
  }

  write.csv(summary_table, csv_output, row.names = FALSE)
  save_release_proximity_table_png(summary_table, png_output, dataset_label)

  cat(sprintf("Release-cycle summary table saved (%s): %s\n", dataset_label, csv_output))
  cat(sprintf("Release-cycle summary image saved (%s): %s\n", dataset_label, png_output))

  # Remove the CSV file after creation
  if (file.exists(csv_output)) {
    file.remove(csv_output)
    cat(sprintf("Release-cycle summary CSV deleted (%s): %s\n", dataset_label, csv_output))
  }
}

get_test_smells_catalog <- function() {
  data.frame(
    smell_id = sprintf("BP%02d", 1:20),
    smell_name = c(
      "Absolute URL",
      "Absolute XPath",
      "Assertion Roulette",
      "Complex Test",
      "Conditional Logic",
      "Constructor Initialization",
      "Duplicate Assert",
      "Empty Test",
      "Exception Handling",
      "Global Variable",
      "Magic Number",
      "Misused Tag Locator",
      "Mystery Guest",
      "Non-Preferred Locator",
      "Redundant Assertion",
      "Redundant Print",
      "Sensitive Equality",
      "Sleepy Test",
      "Unknown Test",
      "Unstable Link Text"
    ),
    stringsAsFactors = FALSE
  )
}

normalize_smell_label <- function(values) {
  x <- tolower(values)
  x <- gsub("detectorjs$", "", x)
  x <- gsub("detectorts$", "", x)
  x <- gsub("detector$", "", x)
  gsub("[^a-z0-9]", "", x)
}

build_commit_variation_lookup <- function(report_frame) {
  if (nrow(report_frame) == 0) {
    return(data.frame(
      repository = character(0),
      file_name = character(0),
      commit_hash = character(0),
      variation_type = character(0),
      stringsAsFactors = FALSE
    ))
  }

  split_key <- paste(report_frame$dataset, report_frame$repository, report_frame$file_name, sep = "||")
  groups <- split(report_frame, split_key)
  classified <- do.call(rbind, lapply(groups, classify_variation))

  classified[, c("repository", "file_name", "commit_hash", "variation_type")]
}

read_historical_smells_rows <- function(db_path) {
  db_resolved <- resolve_input_path(db_path)
  if (!file.exists(db_resolved)) {
    stop(sprintf("Database not found: %s", db_resolved))
  }

  con <- dbConnect(RSQLite::SQLite(), db_resolved)
  on.exit(dbDisconnect(con), add = TRUE)

  query <- "
    SELECT
      repository,
      file AS file_name,
      commit_hash,
      commit_author,
      date,
      nearest_future_release_date,
      nearest_previous_release_date,
      smell_type
    FROM historical_smells
    WHERE smell_type IS NOT NULL
      AND smell_type != 'NO_SMELL'
  "

  frame <- dbGetQuery(con, query)
  if (nrow(frame) == 0) {
    return(frame)
  }

  frame$date <- parse_iso_datetime(frame$date)
  frame$nearest_future_release_date <- parse_iso_datetime(frame$nearest_future_release_date)
  frame$nearest_previous_release_date <- parse_iso_datetime(frame$nearest_previous_release_date)
  frame
}

read_report_developer_rows <- function(db_path) {
  db_resolved <- resolve_input_path(db_path)
  if (!file.exists(db_resolved)) {
    stop(sprintf("Database not found: %s", db_resolved))
  }

  con <- dbConnect(RSQLite::SQLite(), db_resolved)
  on.exit(dbDisconnect(con), add = TRUE)

  query <- "
    SELECT
      repository,
      file_name,
      author,
      is_owner,
      developer_type
    FROM report_developer_details
  "

  frame <- dbGetQuery(con, query)
  if (nrow(frame) == 0) {
    return(frame)
  }

  frame$author <- as.character(frame$author)
  frame$is_owner <- as.integer(ifelse(is.na(frame$is_owner), 0L, frame$is_owner))
  frame$developer_type <- tolower(trimws(as.character(frame$developer_type)))
  frame
}

build_startup_bins_table <- function(report_frame, smells_frame, dataset_label) {
  outcome_order <- c("No-change", "Initial", "Improving", "Worsening")
  outcome_header <- c("No-change", "Introduction", "Improving", "Worsening")
  bin_order <- c("1w", "1m", "1y", ">1y")

  catalog <- get_test_smells_catalog()
  catalog$smell_key <- normalize_smell_label(catalog$smell_name)

  lookup <- build_commit_variation_lookup(report_frame)
  if (nrow(lookup) == 0 || nrow(smells_frame) == 0) {
    return(NULL)
  }

  merged <- merge(
    smells_frame,
    lookup,
    by = c("repository", "file_name", "commit_hash"),
    all.x = FALSE,
    all.y = FALSE
  )
  if (nrow(merged) == 0) {
    return(NULL)
  }

  merged$signed_days <- mapply(
    closest_release_signed_days,
    merged$date,
    merged$nearest_previous_release_date,
    merged$nearest_future_release_date
  )
  merged <- merged[!is.na(merged$signed_days) & !is.na(merged$variation_type), ]
  if (nrow(merged) == 0) {
    return(NULL)
  }

  merged$smell_key <- normalize_smell_label(merged$smell_type)
  merged <- merge(
    merged,
    catalog[, c("smell_id", "smell_name", "smell_key")],
    by = "smell_key",
    all.x = FALSE,
    all.y = FALSE
  )
  if (nrow(merged) == 0) {
    return(NULL)
  }

  abs_days <- abs(merged$signed_days)
  merged$time_bin <- ifelse(
    abs_days <= 7,
    "1w",
    ifelse(abs_days <= 30, "1m", ifelse(abs_days <= 365, "1y", ">1y"))
  )

  present_ids <- unique(merged$smell_id)
  present_ids <- catalog$smell_id[catalog$smell_id %in% present_ids]
  if (length(present_ids) == 0) {
    return(NULL)
  }

  row_index <- c(present_ids, "Total")
  col_index <- as.vector(outer(outcome_order, bin_order, paste, sep = "|"))
  percentages <- matrix(0, nrow = length(row_index), ncol = length(col_index), dimnames = list(row_index, col_index))

  for (smell_id in present_ids) {
    subset_smell <- merged[merged$smell_id == smell_id, ]
    for (outcome in outcome_order) {
      subset_outcome <- subset_smell[subset_smell$variation_type == outcome, ]
      denom <- nrow(subset_outcome)
      if (denom == 0) {
        next
      }
      counts <- table(factor(subset_outcome$time_bin, levels = bin_order))
      percentages[smell_id, paste(outcome, bin_order, sep = "|")] <- round(100 * as.numeric(counts) / denom)
    }
  }

  n_by_outcome <- integer(length(outcome_order))
  names(n_by_outcome) <- outcome_order
  for (outcome in outcome_order) {
    subset_outcome <- merged[merged$variation_type == outcome, ]
    n_by_outcome[[outcome]] <- nrow(subset_outcome)
    denom <- nrow(subset_outcome)
    if (denom == 0) {
      next
    }
    counts <- table(factor(subset_outcome$time_bin, levels = bin_order))
    percentages["Total", paste(outcome, bin_order, sep = "|")] <- round(100 * as.numeric(counts) / denom)
  }

  row_labels <- c(present_ids, "Total")

  list(
    dataset_label = dataset_label,
    outcome_order = outcome_order,
    outcome_header = outcome_header,
    bin_order = bin_order,
    n_by_outcome = n_by_outcome,
    row_labels = row_labels,
    percentages = percentages
  )
}

save_startup_bins_table_png <- function(table_data, output_png) {
  if (is.null(table_data)) {
    return(FALSE)
  }

  output_dir <- dirname(output_png)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  row_labels <- table_data$row_labels
  pct <- table_data$percentages
  outcome_order <- table_data$outcome_order
  outcome_header <- table_data$outcome_header
  bin_order <- table_data$bin_order
  n_by_outcome <- table_data$n_by_outcome

  n_rows <- length(row_labels)
  first_col_w <- 0.16
  body_w <- 0.79
  cell_w <- body_w / (length(outcome_order) * length(bin_order))

  x_left <- 0.03
  x_edges <- c(x_left, x_left + first_col_w, x_left + first_col_w + cumsum(rep(cell_w, length(outcome_order) * length(bin_order))))

  top_margin <- 0.06
  bottom_margin <- 0.05
  caption_h <- 0.16
  header1_h <- 0.09
  header2_h <- 0.08
  body_h <- (1 - top_margin - bottom_margin - caption_h - header1_h - header2_h) / max(n_rows, 1)

  y_top <- 1 - top_margin
  y_caption_bottom <- y_top - caption_h
  y_header1_bottom <- y_caption_bottom - header1_h
  y_header2_bottom <- y_header1_bottom - header2_h
  y_bottom <- y_header2_bottom - n_rows * body_h

  png(output_png, width = 2600, height = max(1200, 260 + as.integer(90 * n_rows)), res = 180)
  grid.newpage()

  grid.rect(
    x = unit(0.5, "npc"),
    y = unit(0.5, "npc"),
    width = unit(1, "npc"),
    height = unit(1, "npc"),
    gp = gpar(fill = "#d9d9d9", col = NA)
  )

  grid.lines(
    x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
    y = unit(c(y_caption_bottom + 0.01, y_caption_bottom + 0.01), "npc"),
    gp = gpar(col = "black", lwd = 2.2)
  )

  grid.text(
    "Bad\npractice",
    x = unit((x_edges[1] + x_edges[2]) / 2, "npc"),
    y = unit((y_caption_bottom + y_header2_bottom) / 2, "npc"),
    gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.28)
  )

  group_size <- length(bin_order)
  for (g in seq_along(outcome_order)) {
    start_idx <- (g - 1) * group_size + 1
    end_idx <- g * group_size
    group_x_left <- x_edges[2 + start_idx - 1]
    group_x_right <- x_edges[2 + end_idx]
    group_center <- (group_x_left + group_x_right) / 2

    header_text <- sprintf("%s (n=%d)", outcome_header[[g]], as.integer(n_by_outcome[[outcome_order[[g]]]]))
    grid.text(
      header_text,
      x = unit(group_center, "npc"),
      y = unit((y_caption_bottom + y_header1_bottom) / 2, "npc"),
      gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.2)
    )

    grid.lines(
      x = unit(c(group_x_left + 0.01, group_x_right - 0.01), "npc"),
      y = unit(c(y_header1_bottom + 0.004, y_header1_bottom + 0.004), "npc"),
      gp = gpar(col = "black", lwd = 1.0)
    )

    for (b in seq_along(bin_order)) {
      col_idx <- start_idx + b - 1
      x_center <- (x_edges[2 + col_idx - 1] + x_edges[2 + col_idx]) / 2
      grid.text(
        bin_order[[b]],
        x = unit(x_center, "npc"),
        y = unit((y_header1_bottom + y_header2_bottom) / 2, "npc"),
        gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.14)
      )
    }
  }

  grid.lines(
    x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
    y = unit(c(y_header2_bottom, y_header2_bottom), "npc"),
    gp = gpar(col = "black", lwd = 1.6)
  )

  for (i in seq_len(n_rows)) {
    y_center <- y_header2_bottom - (i - 0.5) * body_h
    is_total <- row_labels[[i]] == "Total"

    grid.text(
      row_labels[[i]],
      x = unit(x_edges[1] + 0.008, "npc"),
      y = unit(y_center, "npc"),
      just = c("left", "center"),
      gp = gpar(fontfamily = "serif", fontface = if (is_total) "bold" else "plain", cex = 1.22)
    )

    for (g in seq_along(outcome_order)) {
      for (b in seq_along(bin_order)) {
        col_key <- paste(outcome_order[[g]], bin_order[[b]], sep = "|")
        col_idx <- (g - 1) * length(bin_order) + b
        x_center <- (x_edges[2 + col_idx - 1] + x_edges[2 + col_idx]) / 2
        grid.text(
          sprintf("%d%%", as.integer(pct[row_labels[[i]], col_key])),
          x = unit(x_center, "npc"),
          y = unit(y_center, "npc"),
          gp = gpar(fontfamily = "serif", cex = 1.15)
        )
      }
    }

    y_line <- y_header2_bottom - i * body_h
    if (is_total) {
      grid.lines(
        x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
        y = unit(c(y_line, y_line), "npc"),
        gp = gpar(col = "black", lwd = 1.6)
      )
    } else {
      grid.lines(
        x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
        y = unit(c(y_line, y_line), "npc"),
        gp = gpar(col = "#6b6b6b", lwd = 0.8)
      )
    }
  }

  grid.lines(
    x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
    y = unit(c(y_bottom, y_bottom), "npc"),
    gp = gpar(col = "black", lwd = 2.2)
  )

  dev.off()
  TRUE
}

save_startup_bins_table <- function(js_db_path, ts_db_path, project_root) {
  js_report <- read_report_commits(js_db_path)
  ts_report <- read_report_commits(ts_db_path)
  js_smells <- read_historical_smells_rows(js_db_path)
  ts_smells <- read_historical_smells_rows(ts_db_path)

  js_table <- build_startup_bins_table(js_report, js_smells, "JS")
  ts_table <- build_startup_bins_table(ts_report, ts_smells, "TS")

  output_js <- normalizePath(
    file.path(project_root, "analyses", "plots", "startup_bins_by_bp_variation_js_R.png"),
    winslash = "/",
    mustWork = FALSE
  )
  output_ts <- normalizePath(
    file.path(project_root, "analyses", "plots", "startup_bins_by_bp_variation_ts_R.png"),
    winslash = "/",
    mustWork = FALSE
  )

  js_ok <- save_startup_bins_table_png(js_table, output_js)
  ts_ok <- save_startup_bins_table_png(ts_table, output_ts)

  if (js_ok) cat(sprintf("Startup-bin table image saved (JS): %s\n", output_js))
  if (ts_ok) cat(sprintf("Startup-bin table image saved (TS): %s\n", output_ts))
}

build_ownership_newcomer_table <- function(report_frame, smells_frame, developer_frame, dataset_label) {
  outcome_order <- c("No-change", "Initial", "Improving", "Worsening")
  outcome_header <- c("No-change", "Introduction", "Improving", "Worsening")

  catalog <- get_test_smells_catalog()
  catalog$smell_key <- normalize_smell_label(catalog$smell_name)

  lookup <- build_commit_variation_lookup(report_frame)
  if (nrow(lookup) == 0 || nrow(smells_frame) == 0 || nrow(developer_frame) == 0) {
    return(NULL)
  }

  merged <- merge(
    smells_frame,
    lookup,
    by = c("repository", "file_name", "commit_hash"),
    all.x = FALSE,
    all.y = FALSE
  )
  if (nrow(merged) == 0) {
    return(NULL)
  }

  merged <- merged[!is.na(merged$variation_type), ]
  if (nrow(merged) == 0) {
    return(NULL)
  }

  merged$smell_key <- normalize_smell_label(merged$smell_type)
  merged <- merge(
    merged,
    catalog[, c("smell_id", "smell_name", "smell_key")],
    by = "smell_key",
    all.x = FALSE,
    all.y = FALSE
  )
  if (nrow(merged) == 0) {
    return(NULL)
  }

  authored <- merge(
    merged,
    developer_frame,
    by.x = c("repository", "file_name", "commit_author"),
    by.y = c("repository", "file_name", "author"),
    all.x = FALSE,
    all.y = FALSE
  )
  if (nrow(authored) == 0) {
    return(NULL)
  }

  present_ids <- unique(authored$smell_id)
  present_ids <- catalog$smell_id[catalog$smell_id %in% present_ids]
  if (length(present_ids) == 0) {
    return(NULL)
  }

  row_labels <- c(present_ids, "Total")
  owner_pct <- matrix(0, nrow = length(row_labels), ncol = length(outcome_order), dimnames = list(row_labels, outcome_order))
  newcomer_pct <- matrix(0, nrow = length(row_labels), ncol = length(outcome_order), dimnames = list(row_labels, outcome_order))

  for (smell_id in present_ids) {
    subset_smell <- authored[authored$smell_id == smell_id, ]
    for (outcome in outcome_order) {
      subset_outcome <- subset_smell[subset_smell$variation_type == outcome, ]
      denom <- nrow(subset_outcome)
      if (denom == 0) {
        next
      }
      owner_pct[smell_id, outcome] <- round(100 * mean(subset_outcome$is_owner == 1L))
      newcomer_pct[smell_id, outcome] <- round(100 * mean(subset_outcome$developer_type == "newcomer"))
    }
  }

  n_by_outcome <- integer(length(outcome_order))
  names(n_by_outcome) <- outcome_order
  for (outcome in outcome_order) {
    subset_outcome <- authored[authored$variation_type == outcome, ]
    n_by_outcome[[outcome]] <- nrow(subset_outcome)
    denom <- nrow(subset_outcome)
    if (denom == 0) {
      next
    }
    owner_pct["Total", outcome] <- round(100 * mean(subset_outcome$is_owner == 1L))
    newcomer_pct["Total", outcome] <- round(100 * mean(subset_outcome$developer_type == "newcomer"))
  }

  list(
    dataset_label = dataset_label,
    outcome_order = outcome_order,
    outcome_header = outcome_header,
    n_by_outcome = n_by_outcome,
    row_labels = row_labels,
    owner_pct = owner_pct,
    newcomer_pct = newcomer_pct
  )
}

save_ownership_newcomer_table_png <- function(table_data, output_png) {
  if (is.null(table_data)) {
    return(FALSE)
  }

  output_dir <- dirname(output_png)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  row_labels <- table_data$row_labels
  outcome_order <- table_data$outcome_order
  outcome_header <- table_data$outcome_header
  n_by_outcome <- table_data$n_by_outcome
  owner_pct <- table_data$owner_pct
  newcomer_pct <- table_data$newcomer_pct

  n_rows <- length(row_labels)
  first_col_w <- 0.14
  body_w <- 0.82
  subcols_per_group <- 2
  total_subcols <- length(outcome_order) * subcols_per_group
  cell_w <- body_w / total_subcols

  x_left <- 0.035
  x_edges <- c(
    x_left,
    x_left + first_col_w,
    x_left + first_col_w + cumsum(rep(cell_w, total_subcols))
  )

  top_margin <- 0.08
  bottom_margin <- 0.06
  header1_h <- 0.09
  header2_h <- 0.10
  body_h <- (1 - top_margin - bottom_margin - header1_h - header2_h) / max(n_rows, 1)

  y_top <- 1 - top_margin
  y_header1_bottom <- y_top - header1_h
  y_header2_bottom <- y_header1_bottom - header2_h
  y_bottom <- y_header2_bottom - n_rows * body_h

  png(output_png, width = 2600, height = max(1200, 280 + as.integer(90 * n_rows)), res = 180)
  grid.newpage()

  grid.rect(
    x = unit(0.5, "npc"),
    y = unit(0.5, "npc"),
    width = unit(1, "npc"),
    height = unit(1, "npc"),
    gp = gpar(fill = "#d9d9d9", col = NA)
  )

  grid.lines(
    x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
    y = unit(c(y_top, y_top), "npc"),
    gp = gpar(col = "black", lwd = 2.8)
  )

  grid.text(
    "Bad\npractice",
    x = unit((x_edges[1] + x_edges[2]) / 2, "npc"),
    y = unit((y_top + y_header2_bottom) / 2, "npc"),
    gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.35)
  )

  for (g in seq_along(outcome_order)) {
    start_idx <- (g - 1) * subcols_per_group + 1
    end_idx <- g * subcols_per_group
    group_x_left <- x_edges[2 + start_idx - 1]
    group_x_right <- x_edges[2 + end_idx]
    group_center <- (group_x_left + group_x_right) / 2

    header_text <- sprintf("%s (n=%d)", outcome_header[[g]], as.integer(n_by_outcome[[outcome_order[[g]]]]))
    grid.text(
      header_text,
      x = unit(group_center, "npc"),
      y = unit((y_top + y_header1_bottom) / 2, "npc"),
      gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.28)
    )

    grid.lines(
      x = unit(c(group_x_left + 0.008, group_x_right - 0.008), "npc"),
      y = unit(c(y_header1_bottom + 0.004, y_header1_bottom + 0.004), "npc"),
      gp = gpar(col = "black", lwd = 1.2)
    )

    x_owner <- (x_edges[2 + start_idx - 1] + x_edges[2 + start_idx]) / 2
    x_newc <- (x_edges[2 + end_idx - 1] + x_edges[2 + end_idx]) / 2

    grid.text(
      "Owner\nTrue (%)",
      x = unit(x_owner, "npc"),
      y = unit((y_header1_bottom + y_header2_bottom) / 2, "npc"),
      gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.16)
    )
    grid.text(
      "Newcomer\nTrue (%)",
      x = unit(x_newc, "npc"),
      y = unit((y_header1_bottom + y_header2_bottom) / 2, "npc"),
      gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.16)
    )
  }

  grid.lines(
    x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
    y = unit(c(y_header2_bottom, y_header2_bottom), "npc"),
    gp = gpar(col = "black", lwd = 1.5)
  )

  for (i in seq_len(n_rows)) {
    y_center <- y_header2_bottom - (i - 0.5) * body_h
    is_total <- row_labels[[i]] == "Total"

    grid.text(
      row_labels[[i]],
      x = unit(x_edges[1] + 0.008, "npc"),
      y = unit(y_center, "npc"),
      just = c("left", "center"),
      gp = gpar(fontfamily = "serif", fontface = if (is_total) "bold" else "plain", cex = 1.28)
    )

    for (g in seq_along(outcome_order)) {
      start_idx <- (g - 1) * subcols_per_group + 1
      end_idx <- g * subcols_per_group
      x_owner <- (x_edges[2 + start_idx - 1] + x_edges[2 + start_idx]) / 2
      x_newc <- (x_edges[2 + end_idx - 1] + x_edges[2 + end_idx]) / 2

      grid.text(
        sprintf("%d", as.integer(owner_pct[row_labels[[i]], outcome_order[[g]]])),
        x = unit(x_owner, "npc"),
        y = unit(y_center, "npc"),
        gp = gpar(fontfamily = "serif", cex = 1.22)
      )
      grid.text(
        sprintf("%d", as.integer(newcomer_pct[row_labels[[i]], outcome_order[[g]]])),
        x = unit(x_newc, "npc"),
        y = unit(y_center, "npc"),
        gp = gpar(fontfamily = "serif", cex = 1.22)
      )
    }

    y_line <- y_header2_bottom - i * body_h
    if (is_total) {
      grid.lines(
        x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
        y = unit(c(y_line, y_line), "npc"),
        gp = gpar(col = "black", lwd = 1.5)
      )
    }
  }

  grid.lines(
    x = unit(c(x_left, x_edges[length(x_edges)]), "npc"),
    y = unit(c(y_bottom, y_bottom), "npc"),
    gp = gpar(col = "black", lwd = 2.8)
  )

  dev.off()
  TRUE
}

save_ownership_newcomer_tables <- function(js_db_path, ts_db_path, project_root) {
  js_report <- read_report_commits(js_db_path)
  ts_report <- read_report_commits(ts_db_path)
  js_smells <- read_historical_smells_rows(js_db_path)
  ts_smells <- read_historical_smells_rows(ts_db_path)
  js_developers <- read_report_developer_rows(js_db_path)
  ts_developers <- read_report_developer_rows(ts_db_path)

  js_table <- build_ownership_newcomer_table(js_report, js_smells, js_developers, "JS")
  ts_table <- build_ownership_newcomer_table(ts_report, ts_smells, ts_developers, "TS")

  output_js <- normalizePath(
    file.path(project_root, "analyses", "plots", "ownership_newcomer_by_bp_variation_js_R.png"),
    winslash = "/",
    mustWork = FALSE
  )
  output_ts <- normalizePath(
    file.path(project_root, "analyses", "plots", "ownership_newcomer_by_bp_variation_ts_R.png"),
    winslash = "/",
    mustWork = FALSE
  )

  js_ok <- save_ownership_newcomer_table_png(js_table, output_js)
  ts_ok <- save_ownership_newcomer_table_png(ts_table, output_ts)

  if (js_ok) cat(sprintf("Ownership/Newcomer table image saved (JS): %s\n", output_js))
  if (ts_ok) cat(sprintf("Ownership/Newcomer table image saved (TS): %s\n", output_ts))
}

save_smells_catalog_table_png <- function(output_png) {
  output_dir <- dirname(output_png)
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  smells <- get_test_smells_catalog()$smell_name

  smell_ids <- sprintf("BP%02d", seq_along(smells))

  wrap_cell <- function(text, width = 34) {
    paste(strwrap(text, width = width), collapse = "\n")
  }

  wrapped_smells <- vapply(smells, wrap_cell, character(1))
  line_counts <- vapply(strsplit(wrapped_smells, "\n", fixed = TRUE), length, integer(1))

  col_widths <- c(0.14, 0.80)
  x_edges <- c(0.03, 0.03 + cumsum(col_widths))
  right_margin <- 0.03

  top_margin <- 0.02
  bottom_margin <- 0.035
  header_h <- 0.085
  row_units <- line_counts + 0.35
  available_h <- 1 - top_margin - bottom_margin - header_h
  row_heights <- (row_units / sum(row_units)) * available_h

  y_top <- 1 - top_margin
  y_header_bottom <- y_top - header_h

  png(output_png, width = 1500, height = 1600, res = 180)
  grid.newpage()

  grid.rect(
    x = unit(0.5, "npc"),
    y = unit(0.5, "npc"),
    width = unit(1, "npc"),
    height = unit(1, "npc"),
    gp = gpar(fill = "#d9d9d9", col = NA)
  )

  grid.lines(
    x = unit(c(x_edges[1], x_edges[3]), "npc"),
    y = unit(c(y_top, y_top), "npc"),
    gp = gpar(col = "black", lwd = 2.6)
  )

  grid.lines(
    x = unit(c(x_edges[1], x_edges[3]), "npc"),
    y = unit(c(y_header_bottom, y_header_bottom), "npc"),
    gp = gpar(col = "#5f5f5f", lwd = 1.7)
  )

  grid.text(
    "ID",
    x = unit(x_edges[1] + 0.012, "npc"),
    y = unit((y_top + y_header_bottom) / 2, "npc"),
    just = c("left", "center"),
    gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.45)
  )

  grid.text(
    "Bad Practice",
    x = unit(x_edges[3] - right_margin, "npc"),
    y = unit((y_top + y_header_bottom) / 2, "npc"),
    just = c("right", "center"),
    gp = gpar(fontfamily = "serif", fontface = "bold", cex = 1.45)
  )

  y_cursor <- y_header_bottom
  for (i in seq_along(smells)) {
    row_h <- row_heights[[i]]
    y_next <- y_cursor - row_h
    y_center <- (y_cursor + y_next) / 2

    grid.text(
      smell_ids[[i]],
      x = unit(x_edges[1] + 0.012, "npc"),
      y = unit(y_center, "npc"),
      just = c("left", "center"),
      gp = gpar(fontfamily = "serif", cex = 1.42)
    )

    grid.text(
      wrapped_smells[[i]],
      x = unit(x_edges[3] - right_margin, "npc"),
      y = unit(y_center, "npc"),
      just = c("right", "center"),
      gp = gpar(fontfamily = "serif", cex = 1.38, lineheight = 1.05)
    )

    grid.lines(
      x = unit(c(x_edges[1], x_edges[3]), "npc"),
      y = unit(c(y_next, y_next), "npc"),
      gp = gpar(col = "#4f4f4f", lwd = 1.5)
    )

    y_cursor <- y_next
  }

  grid.lines(
    x = unit(c(x_edges[1], x_edges[3]), "npc"),
    y = unit(c(y_cursor, y_cursor), "npc"),
    gp = gpar(col = "black", lwd = 2.6)
  )

  dev.off()
}

read_historical_smells_incidence <- function(db_path, language_label) {
  db_resolved <- resolve_input_path(db_path)
  if (!file.exists(db_resolved)) {
    stop(sprintf("Database not found: %s", db_resolved))
  }

  con <- dbConnect(RSQLite::SQLite(), db_resolved)
  on.exit(dbDisconnect(con), add = TRUE)

  query <- "
    SELECT
      framework,
      repository,
      file AS file_name,
      smell_type
    FROM historical_smells
    WHERE smell_type IS NOT NULL
      AND TRIM(smell_type) != ''
      AND smell_type != 'NO_SMELL'
  "

  frame <- dbGetQuery(con, query)
  if (nrow(frame) == 0) {
    return(data.frame(
      language = character(0),
      framework = character(0),
      repository = character(0),
      file_name = character(0),
      smell_type = character(0),
      test_id = character(0),
      stringsAsFactors = FALSE
    ))
  }

  frame$language <- language_label
  frame$framework <- trimws(as.character(frame$framework))
  frame$framework[is.na(frame$framework) | frame$framework == ""] <- "Unknown"
  frame$repository <- as.character(frame$repository)
  frame$file_name <- as.character(frame$file_name)
  frame$smell_type <- trimws(as.character(frame$smell_type))

  catalog <- get_test_smells_catalog()
  catalog$smell_key <- normalize_smell_label(catalog$smell_name)
  smell_key <- normalize_smell_label(frame$smell_type)
  match_index <- match(smell_key, catalog$smell_key)
  canonical <- catalog$smell_name[match_index]
  frame$smell_type <- ifelse(is.na(canonical), frame$smell_type, canonical)

  frame$test_id <- paste(frame$repository, frame$file_name, sep = "||")

  frame[, c("language", "framework", "repository", "file_name", "smell_type", "test_id")]
}

build_smell_incidence_by_language <- function(frame) {
  if (nrow(frame) == 0) {
    return(data.frame(
      smell_type = character(0),
      distinct_tests_total = integer(0),
      occurrences_total = integer(0),
      occurrences_javascript = integer(0),
      distinct_tests_javascript = integer(0),
      occurrences_typescript = integer(0),
      distinct_tests_typescript = integer(0),
      stringsAsFactors = FALSE
    ))
  }

  smells <- sort(unique(frame$smell_type))
  result <- data.frame(smell_type = smells, stringsAsFactors = FALSE)

  total_occ <- aggregate(list(occurrences_total = frame$smell_type), by = list(smell_type = frame$smell_type), FUN = length)
  total_tests <- aggregate(
    list(distinct_tests_total = unique(frame[, c("smell_type", "test_id")])$test_id),
    by = list(smell_type = unique(frame[, c("smell_type", "test_id")])$smell_type),
    FUN = length
  )

  js_frame <- frame[tolower(frame$language) == "javascript", ]
  ts_frame <- frame[tolower(frame$language) == "typescript", ]

  if (nrow(js_frame) > 0) {
    js_occ <- aggregate(list(occurrences_javascript = js_frame$smell_type), by = list(smell_type = js_frame$smell_type), FUN = length)
    js_unique <- unique(js_frame[, c("smell_type", "test_id")])
    js_tests <- aggregate(list(distinct_tests_javascript = js_unique$test_id), by = list(smell_type = js_unique$smell_type), FUN = length)
  } else {
    js_occ <- data.frame(smell_type = character(0), occurrences_javascript = integer(0), stringsAsFactors = FALSE)
    js_tests <- data.frame(smell_type = character(0), distinct_tests_javascript = integer(0), stringsAsFactors = FALSE)
  }

  if (nrow(ts_frame) > 0) {
    ts_occ <- aggregate(list(occurrences_typescript = ts_frame$smell_type), by = list(smell_type = ts_frame$smell_type), FUN = length)
    ts_unique <- unique(ts_frame[, c("smell_type", "test_id")])
    ts_tests <- aggregate(list(distinct_tests_typescript = ts_unique$test_id), by = list(smell_type = ts_unique$smell_type), FUN = length)
  } else {
    ts_occ <- data.frame(smell_type = character(0), occurrences_typescript = integer(0), stringsAsFactors = FALSE)
    ts_tests <- data.frame(smell_type = character(0), distinct_tests_typescript = integer(0), stringsAsFactors = FALSE)
  }

  result <- merge(result, total_tests, by = "smell_type", all.x = TRUE)
  result <- merge(result, total_occ, by = "smell_type", all.x = TRUE)
  result <- merge(result, js_occ, by = "smell_type", all.x = TRUE)
  result <- merge(result, js_tests, by = "smell_type", all.x = TRUE)
  result <- merge(result, ts_occ, by = "smell_type", all.x = TRUE)
  result <- merge(result, ts_tests, by = "smell_type", all.x = TRUE)

  numeric_cols <- setdiff(names(result), "smell_type")
  for (col_name in numeric_cols) {
    result[[col_name]][is.na(result[[col_name]])] <- 0L
    result[[col_name]] <- as.integer(result[[col_name]])
  }

  result[order(result$occurrences_total, decreasing = TRUE), ]
}

build_smell_incidence_by_framework <- function(frame) {
  if (nrow(frame) == 0) {
    return(data.frame(
      language = character(0),
      framework = character(0),
      smell_type = character(0),
      distinct_tests = integer(0),
      occurrences = integer(0),
      stringsAsFactors = FALSE
    ))
  }

  occurrences <- aggregate(
    list(occurrences = frame$smell_type),
    by = list(language = frame$language, framework = frame$framework, smell_type = frame$smell_type),
    FUN = length
  )

  distinct_rows <- unique(frame[, c("language", "framework", "smell_type", "test_id")])
  tests <- aggregate(
    list(distinct_tests = distinct_rows$test_id),
    by = list(language = distinct_rows$language, framework = distinct_rows$framework, smell_type = distinct_rows$smell_type),
    FUN = length
  )

  result <- merge(occurrences, tests, by = c("language", "framework", "smell_type"), all.x = TRUE)
  result$distinct_tests[is.na(result$distinct_tests)] <- 0L
  result$distinct_tests <- as.integer(result$distinct_tests)
  result$occurrences <- as.integer(result$occurrences)
  result[order(result$language, result$framework, -result$occurrences, result$smell_type), ]
}

save_bad_smell_incidence_tables <- function(js_db_path, ts_db_path, project_root) {
  js_rows <- read_historical_smells_incidence(js_db_path, "JavaScript")
  ts_rows <- read_historical_smells_incidence(ts_db_path, "TypeScript")
  all_rows <- rbind(js_rows, ts_rows)

  if (nrow(all_rows) == 0) {
    stop("No bad smell rows found in historical_smells for JS/TS databases.")
  }

  language_table <- build_smell_incidence_by_language(all_rows)
  framework_table <- build_smell_incidence_by_framework(all_rows)

  reports_dir <- normalizePath(file.path(project_root, "analyses", "reports"), winslash = "/", mustWork = FALSE)
  if (!dir.exists(reports_dir)) {
    dir.create(reports_dir, recursive = TRUE)
  }

  out_language <- normalizePath(
    file.path(reports_dir, "bad_smells_incidence_language_R.csv"),
    winslash = "/",
    mustWork = FALSE
  )
  out_framework <- normalizePath(
    file.path(reports_dir, "bad_smells_incidence_framework_R.csv"),
    winslash = "/",
    mustWork = FALSE
  )

  write.csv(language_table, out_language, row.names = FALSE)
  write.csv(framework_table, out_framework, row.names = FALSE)

  cat(sprintf("Bad-smell incidence table saved (language): %s\n", out_language))
  cat(sprintf("Bad-smell incidence table saved (framework): %s\n", out_framework))
}

args <- commandArgs(trailingOnly = TRUE)

js_db <- "historical_smellsJS.db"
ts_db <- "historical_smellsTS.db"
figure_width <- 11.5
figure_height <- 7.0
bw_adjust <- 1.0
dpi <- 220
smells_only <- FALSE
startup_tables_only <- FALSE
ownership_tables_only <- FALSE
ridge_release_only <- FALSE
incidence_only <- FALSE

if (length(args) > 0) {
  i <- 1
  while (i <= length(args)) {
    if (args[[i]] == "--js-db" && i + 1 <= length(args)) {
      js_db <- args[[i + 1]]
      i <- i + 2
    } else if (args[[i]] == "--ts-db" && i + 1 <= length(args)) {
      ts_db <- args[[i + 1]]
      i <- i + 2
    } else if (args[[i]] == "--figure-width" && i + 1 <= length(args)) {
      figure_width <- as.numeric(args[[i + 1]])
      i <- i + 2
    } else if (args[[i]] == "--figure-height" && i + 1 <= length(args)) {
      figure_height <- as.numeric(args[[i + 1]])
      i <- i + 2
    } else if (args[[i]] == "--bw-adjust" && i + 1 <= length(args)) {
      bw_adjust <- as.numeric(args[[i + 1]])
      i <- i + 2
    } else if (args[[i]] == "--dpi" && i + 1 <= length(args)) {
      dpi <- as.integer(args[[i + 1]])
      i <- i + 2
    } else if (args[[i]] == "--smells-only") {
      smells_only <- TRUE
      i <- i + 1
    } else if (args[[i]] == "--startup-tables-only") {
      startup_tables_only <- TRUE
      i <- i + 1
    } else if (args[[i]] == "--ownership-tables-only") {
      ownership_tables_only <- TRUE
      i <- i + 1
    } else if (args[[i]] == "--ridge-release-only") {
      ridge_release_only <- TRUE
      i <- i + 1
    } else if (args[[i]] == "--incidence-only") {
      incidence_only <- TRUE
      i <- i + 1
    } else {
      i <- i + 1
    }
  }
}

if (smells_only) {
  smells_table_output <- normalizePath(
    file.path(PROJECT_ROOT, "analyses", "plots", "test_smells_catalog_table_R.png"),
    winslash = "/",
    mustWork = FALSE
  )
  save_smells_catalog_table_png(smells_table_output)
  cat(sprintf("Smells catalog table image saved: %s\n", smells_table_output))
  quit(save = "no", status = 0)
}

if (startup_tables_only) {
  save_startup_bins_table(js_db, ts_db, PROJECT_ROOT)
  quit(save = "no", status = 0)
}

if (ownership_tables_only) {
  save_ownership_newcomer_tables(js_db, ts_db, PROJECT_ROOT)
  quit(save = "no", status = 0)
}

if (incidence_only) {
  save_bad_smell_incidence_tables(js_db, ts_db, PROJECT_ROOT)
  quit(save = "no", status = 0)
}

js_df <- read_report_commits(js_db)
ts_df <- read_report_commits(ts_db)

plot_data_js <- build_plot_data(js_df)
output_js <- normalizePath(file.path(PROJECT_ROOT, "analyses", "plots", "release_distance_ridge_js_R.png"), winslash = "/", mustWork = FALSE)
plot_ridge_manual(plot_data_js, output_js, figure_width, figure_height, bw_adjust, dpi)
print_counts(plot_data_js, "JS")
save_release_proximity_summary(plot_data_js, "JS", PROJECT_ROOT)

plot_data_ts <- build_plot_data(ts_df)
output_ts <- normalizePath(file.path(PROJECT_ROOT, "analyses", "plots", "release_distance_ridge_ts_R.png"), winslash = "/", mustWork = FALSE)
plot_ridge_manual(plot_data_ts, output_ts, figure_width, figure_height, bw_adjust, dpi)
print_counts(plot_data_ts, "TS")
save_release_proximity_summary(plot_data_ts, "TS", PROJECT_ROOT)

if (ridge_release_only) {
  quit(save = "no", status = 0)
}

smells_table_output <- normalizePath(
  file.path(PROJECT_ROOT, "analyses", "plots", "test_smells_catalog_table_R.png"),
  winslash = "/",
  mustWork = FALSE
)
save_smells_catalog_table_png(smells_table_output)
cat(sprintf("Smells catalog table image saved: %s\n", smells_table_output))

save_startup_bins_table(js_db, ts_db, PROJECT_ROOT)
save_ownership_newcomer_tables(js_db, ts_db, PROJECT_ROOT)
save_bad_smell_incidence_tables(js_db, ts_db, PROJECT_ROOT)
