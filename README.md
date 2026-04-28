# 🌦️ World Weather Analytics Dashboard

An end-to-end automated data pipeline that pulls weather data from Kaggle,
engineers features, trains an ML model tracked with MLflow, and serves an
interactive Shiny dashboard — all orchestrated with Docker and GitHub Actions.

---

## Architecture

```
Kaggle API
   │  (daily via GitHub Actions cron)
   ▼
data/raw/GlobalWeatherRepository.csv
   │
   ▼  transform/build_db.R
   │  • Country normalisation
   │  • Continent & subregion mapping
   │  • Heat index, wind chill, comfort index
   │  • Season, AQI category, humidity/precip bands
   │  • DuckDB indices for fast queries
   ▼
data/curated/weather.duckdb
   │
   ├──▶  transform/train_model.R
   │     • tidymodels Random Forest
   │     • 5-fold CV hyperparameter tuning
   │     • MLflow experiment tracking
   │     └──▶ data/curated/weather_model.rds
   │
   └──▶  shiny_app/app.R
         • Overview heatmaps & charts
         • UMAP / t-SNE dimensionality reduction
         • Country similarity network (igraph)
         • Animated temporal trends (gganimate)
         • Eastern Africa case study
         • Data explorer (DT)
         • ML insights tab
         └──▶ shinyapps.io (free tier)
```

---

## Quick Start (Local)

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- A [Kaggle account](https://www.kaggle.com) with API token
- A [shinyapps.io account](https://www.shinyapps.io) (free)

### 1. Clone & configure

```bash
git clone https://github.com/<your-username>/World-weather-dashboard.git
cd World-weather-dashboard
cp .env.example .env
# Edit .env with your Kaggle and shinyapps.io credentials
```

### 2. Run the full pipeline locally

```bash
docker compose up
```

This will:
1. Pull the latest Kaggle dataset into `data/raw/`
2. Build the DuckDB database with all feature engineering
3. Train the Random Forest model and log to MLflow
4. Serve the Shiny dashboard at **http://localhost:3838**
5. Serve the MLflow UI at **http://localhost:5000**

### 3. Run steps individually

```bash
# Ingest only
docker compose run --rm ingest

# Transform + train only
docker compose run --rm transform

# Shiny only (assumes curated data already exists)
docker compose up shiny
```

---

## Running Without Docker (R + Python directly)

```bash
# 1. Set Kaggle credentials
export KAGGLE_USERNAME=your_username
export KAGGLE_KEY=your_key

# 2. Ingest
pip install kaggle
python ingest/ingest.py

# 3. Build DuckDB
Rscript transform/build_db.R

# 4. Train model
pip install mlflow
Rscript transform/train_model.R

# 5. View MLflow UI
mlflow ui --backend-store-uri ./mlflow_tracking

# 6. Launch Shiny
Rscript -e "shiny::runApp('shiny_app')"
```

---

## GitHub Actions Automation

Two workflows run automatically:

| Workflow | File | Trigger | What it does |
|---|---|---|---|
| Ingest → Transform → Train | `ingest.yml` | Daily 06:00 UTC | Pulls Kaggle data, rebuilds DuckDB, retrains model, commits results |
| Deploy | `deploy.yml` | Push to `main` (shiny_app/ or data/curated/ changed) | Deploys to shinyapps.io |

### Required GitHub Secrets

Go to **Settings → Secrets and variables → Actions** and add:

| Secret | Where to get it |
|---|---|
| `KAGGLE_USERNAME` | [kaggle.com/settings](https://www.kaggle.com/settings) → API |
| `KAGGLE_KEY` | Same page → Create New Token → `kaggle.json` |
| `SHINYAPPS_ACCOUNT` | [shinyapps.io](https://www.shinyapps.io) → Account → Tokens |
| `SHINYAPPS_TOKEN` | Same page → Add Token |
| `SHINYAPPS_SECRET` | Same page |

---

## MLflow Experiment Tracking

MLflow runs are stored in `mlflow_tracking/` and uploaded as GitHub Actions
artefacts after each training run (retained 30 days).

To view locally:
```bash
mlflow ui --backend-store-uri ./mlflow_tracking
# → http://localhost:5000
```

Each run logs:
- **Parameters**: model type, trees, mtry, min_n, train/test row counts
- **Metrics**: RMSE, R², MAE on held-out test set
- **Artefacts**: `weather_model.rds`, feature importance PNG

---

## Feature Engineering

`transform/build_db.R` adds the following engineered features on top of the raw data:

| Feature | Description |
|---|---|
| `continent` | Derived from country name lookup |
| `subregion` | African subregion (5 regions) |
| `season` | Hemisphere-aware season |
| `month`, `hour`, `day_of_week`, `week_of_year` | Temporal breakdowns |
| `heat_index` | Rothfusz equation (valid when T > 27°C, RH > 40%) |
| `wind_chill` | Wind chill index (valid when T < 10°C, wind > 4.8 km/h) |
| `comfort_index` | Feels-like temperature (heat index or wind chill as appropriate) |
| `wind_cardinal` | 8-point cardinal wind direction from degrees |
| `is_extreme_temp` | Flag for T > 40°C or T < −10°C |
| `aqi_category` | US EPA 6-level air quality label |
| `humidity_category` | Dry / Comfortable / Humid / Very Humid |
| `precip_category` | None / Light / Moderate / Heavy / Extreme |

---

## Dashboard Tabs

| Tab | What it shows |
|---|---|
| **Overview** | KPI boxes, heatmap, country/subregion bar charts, AQI distribution |
| **High-Dim Analysis** | UMAP or t-SNE scatter coloured by any feature |
| **Similarity Network** | igraph network of locations by weather similarity (Louvain communities) |
| **Temporal Trends** | Animated bubble chart (gganimate) across months |
| **EA Network Case Study** | Fixed Eastern Africa network — April 1 2025 |
| **Data Explorer** | Full filterable DT table with regex search |
| **ML Insights** | Predicted vs actual temperature, feature importance, model metrics |

---

## Project Structure

```
.
├── .github/workflows/
│   ├── ingest.yml          # scheduled ingest + transform + train
│   └── deploy.yml          # deploy to shinyapps.io on push
├── ingest/
│   ├── Dockerfile
│   ├── ingest.py           # Kaggle download + MD5 hash check
│   └── requirements.txt
├── transform/
│   ├── Dockerfile
│   ├── build_db.R          # feature engineering → DuckDB
│   └── train_model.R       # Random Forest + MLflow tracking
├── shiny_app/
│   ├── Dockerfile
│   ├── app.R               # full Shiny dashboard
│   └── www/                # static assets + generated GIFs
├── data/
│   ├── raw/                # downloaded CSVs (gitignored)
│   ├── staging/            # intermediate (gitignored)
│   └── curated/            # weather.duckdb + model .rds (committed)
├── mlflow_tracking/        # local MLflow store (gitignored)
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md
```

---

## Free Tier Limits — shinyapps.io

| Resource | Free tier |
|---|---|
| Active hours / month | 25 hours |
| RAM | 1 GB |
| Applications | 5 |
| Custom domains | ✗ |

To stay within limits, the ingest workflow runs once per day and only
re-deploys when the app or data actually changes.

---

## License

MIT — see LICENSE file.
