# =============================================================================
# transform/train_model.R
# Trains a Random Forest to predict temperature_celsius
# Tracks experiments with MLflow (free, local)
# Saves model artefacts to data/curated/
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(tidymodels)
  library(duckdb)
  library(DBI)
  library(mlflow)   # install with: install.packages("mlflow"); mlflow::install_mlflow()
  library(vip)
  library(ranger)
})

cat("=== train_model.R : Starting ===\n")

# --- Paths -------------------------------------------------------------------
DB_PATH      <- file.path("data", "curated", "weather.duckdb")
MODEL_OUT    <- file.path("data", "curated", "weather_model.rds")
METRICS_OUT  <- file.path("data", "curated", "model_metrics.rds")
MLFLOW_DIR   <- "mlflow_tracking"
dir.create(MLFLOW_DIR, recursive = TRUE, showWarnings = FALSE)

# --- 1. Load from DuckDB ----------------------------------------------------
cat("Loading data from DuckDB...\n")
con <- dbConnect(duckdb(), dbdir = DB_PATH, read_only = TRUE)
df  <- dbGetQuery(con, "
  SELECT temperature_celsius, humidity, pressure_mb, precip_mm,
         cloud, wind_degree, wind_mph, air_quality_us_epa_index,
         heat_index, wind_chill, comfort_index,
         continent, subregion, season,
         EXTRACT(month FROM last_updated) AS month_num,
         EXTRACT(hour  FROM last_updated) AS hour_num
  FROM weather_clean
  WHERE temperature_celsius IS NOT NULL
") |> as_tibble()
dbDisconnect(con, shutdown = TRUE)

cat(sprintf("  Rows loaded: %s\n", nrow(df)))

# --- 2. Preprocessing recipe -------------------------------------------------
cat("Building recipe...\n")

df_model <- df |>
  mutate(
    across(c(continent, subregion, season), as.factor),
    month_num = as.integer(month_num),
    hour_num  = as.integer(hour_num)
  ) |>
  drop_na()

set.seed(42)
split   <- initial_split(df_model, prop = 0.8, strata = temperature_celsius)
train_d <- training(split)
test_d  <- testing(split)

rec <- recipe(temperature_celsius ~ ., data = train_d) |>
  step_unknown(all_nominal_predictors()) |>
  step_dummy(all_nominal_predictors(), one_hot = TRUE) |>
  step_zv(all_predictors()) |>
  step_normalize(all_numeric_predictors())

# --- 3. Model spec -----------------------------------------------------------
rf_spec <- rand_forest(
  trees = 300,
  mtry  = tune(),
  min_n = tune()
) |>
  set_engine("ranger", importance = "impurity") |>
  set_mode("regression")

wf <- workflow() |>
  add_recipe(rec) |>
  add_model(rf_spec)

# --- 4. Tune with cross-validation ------------------------------------------
cat("Tuning hyperparameters (5-fold CV)...\n")
folds <- vfold_cv(train_d, v = 5, strata = temperature_celsius)

grid <- grid_regular(
  mtry(range = c(3, 8)),
  min_n(range = c(5, 30)),
  levels = 3
)

tune_res <- tune_grid(
  wf,
  resamples = folds,
  grid      = grid,
  metrics   = metric_set(rmse, rsq, mae)
)

best_params <- select_best(tune_res, metric = "rmse")
cat(sprintf("  Best mtry=%s  min_n=%s\n", best_params$mtry, best_params$min_n))

# --- 5. Final fit ------------------------------------------------------------
cat("Fitting final model...\n")
final_wf    <- finalize_workflow(wf, best_params)
final_fit   <- last_fit(final_wf, split)
final_model <- extract_workflow(final_fit)

# --- 6. Metrics --------------------------------------------------------------
metrics <- collect_metrics(final_fit)
cat("  Test-set metrics:\n")
print(metrics)

# --- 7. MLflow logging -------------------------------------------------------
cat("Logging to MLflow...\n")
mlflow_set_tracking_uri(paste0("file://", normalizePath(MLFLOW_DIR)))
mlflow_set_experiment("weather_temperature_forecast")

with(mlflow_start_run(), {
  # Log parameters
  mlflow_log_param("model_type",  "random_forest")
  mlflow_log_param("trees",       300)
  mlflow_log_param("mtry",        best_params$mtry)
  mlflow_log_param("min_n",       best_params$min_n)
  mlflow_log_param("train_rows",  nrow(train_d))
  mlflow_log_param("test_rows",   nrow(test_d))
  mlflow_log_param("cv_folds",    5)

  # Log metrics
  rmse_val <- metrics |> filter(.metric == "rmse") |> pull(.estimate)
  rsq_val  <- metrics |> filter(.metric == "rsq")  |> pull(.estimate)
  mae_val  <- metrics |> filter(.metric == "mae")   |> pull(.estimate)

  mlflow_log_metric("rmse", rmse_val)
  mlflow_log_metric("rsq",  rsq_val)
  mlflow_log_metric("mae",  mae_val)

  # Log model artifact
  saveRDS(final_model, MODEL_OUT)
  mlflow_log_artifact(MODEL_OUT)

  # Log feature importance plot
  imp_plot_path <- file.path(MLFLOW_DIR, "feature_importance.png")
  png(imp_plot_path, width = 800, height = 500)
  print(
    final_model |>
      extract_fit_parsnip() |>
      vip(num_features = 15) +
      labs(title = "Feature Importance — Random Forest") +
      theme_minimal()
  )
  dev.off()
  mlflow_log_artifact(imp_plot_path)

  cat(sprintf("  MLflow run logged: RMSE=%.3f  R²=%.3f  MAE=%.3f\n",
              rmse_val, rsq_val, mae_val))
})

# --- 8. Save metrics for Shiny -----------------------------------------------
saveRDS(metrics, METRICS_OUT)

cat("=== train_model.R : Done ===\n")
cat(sprintf("  Model  → %s\n", MODEL_OUT))
cat(sprintf("  Metrics→ %s\n", METRICS_OUT))
cat(sprintf("  MLflow → %s/\n", MLFLOW_DIR))
cat("  To view MLflow UI run: mlflow_ui(port=5000) in R, or\n")
cat("  $ mlflow ui --backend-store-uri ./mlflow_tracking\n")
