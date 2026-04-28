# =============================================================================
# transform/build_db.R
# Reads raw CSV from data/raw/, applies feature engineering,
# writes a clean DuckDB database to data/curated/weather.duckdb
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(lubridate)
  library(janitor)
  library(duckdb)
  library(DBI)
})

cat("=== build_db.R : Starting ===\n")

# --- Paths -------------------------------------------------------------------
RAW_CSV    <- file.path("data", "raw", "GlobalWeatherRepository.csv")
DB_PATH    <- file.path("data", "curated", "weather.duckdb")
dir.create(dirname(DB_PATH), recursive = TRUE, showWarnings = FALSE)

# --- 1. Read raw CSV ---------------------------------------------------------
cat("Reading raw CSV...\n")
raw <- read_csv(RAW_CSV, show_col_types = FALSE) |>
  clean_names()

cat(sprintf("  Raw rows: %s  Cols: %s\n", nrow(raw), ncol(raw)))

# --- 2. Country name normalisation ------------------------------------------
cat("Normalising country names...\n")
country_map <- c(
  "bélgica"                             = "Belgium",
  "mexique"                             = "Mexico",
  "malásia"                             = "Malaysia",
  "polônia"                             = "Poland",
  "marrocos"                            = "Morocco",
  "letonia"                             = "Latvia",
  "estonie"                             = "Estonia",
  "türkiye"                             = "Turkey",
  "турция"                              = "Turkey",
  "كولومبيا"                            = "Colombia",
  "гватемала"                           = "Guatemala",
  "火鸡"                                = "Turkey",
  "saudi_arabien"                       = "Saudi Arabia",
  "südkorea"                            = "South Korea",
  "turkménistan"                        = "Turkmenistan",
  "jemen"                               = "Yemen",
  "inde"                                = "India",
  "komoren"                             = "Comoros",
  "польша"                              = "Poland",
  "saint_vincent_et_les_grenadines"     = "Saint Vincent and the Grenadines",
  "usa united states of america"        = "United States of America"
)

df <- raw |>
  mutate(country = recode(tolower(trimws(country)), !!!country_map,
                          .default = country))

# --- 3. Continent mapping ----------------------------------------------------
cat("Adding continent & subregion...\n")
africa    <- c("Algeria","Angola","Benin","Botswana","Burkina Faso","Burundi",
               "Cameroon","Cape Verde","Central African Republic","Chad","Comoros",
               "Congo","Democratic Republic of Congo","Djibouti","Egypt",
               "Equatorial Guinea","Eritrea","Ethiopia","Gabon","Gambia","Ghana",
               "Guinea","Guinea-Bissau","Ivory Coast","Kenya","Lesotho","Liberia",
               "Libya","Madagascar","Malawi","Mali","Mauritania","Mauritius",
               "Morocco","Mozambique","Namibia","Niger","Nigeria","Rwanda","Senegal",
               "Seychelles Islands","Sierra Leone","Somalia","South Africa",
               "South Sudan","Sudan","Swaziland","Tanzania","Togo","Tunisia",
               "Uganda","Zambia","Zimbabwe")

asia      <- c("Afghanistan","Armenia","Azerbaijan","Bahrain","Bangladesh","Bhutan",
               "Brunei Darussalam","Cambodia","China","Cyprus","Georgia","India",
               "Indonesia","Iran","Iraq","Israel","Japan","Jordan","Kazakhstan",
               "North Korea","South Korea","Kuwait","Kyrghyzstan",
               "Lao People's Democratic Republic","Lebanon","Malaysia","Maldives",
               "Mongolia","Myanmar","Nepal","Oman","Pakistan","Palau","Philippines",
               "Qatar","Saudi Arabia","Singapore","Sri Lanka","Syria","Tajikistan",
               "Thailand","Timor-Leste","Turkmenistan","United Arab Emirates",
               "Uzbekistan","Vietnam","Yemen")

europe    <- c("Albania","Andorra","Austria","Belarus","Belgium",
               "Bosnia and Herzegovina","Bulgaria","Croatia","Czech Republic",
               "Denmark","Estonia","Finland","France","Germany","Greece","Hungary",
               "Iceland","Ireland","Italy","Kosovo","Latvia","Lithuania",
               "Luxembourg","Macedonia","Malta","Monaco","Montenegro","Netherlands",
               "Norway","Poland","Portugal","Romania","Russia","Serbia","Slovakia",
               "Slovenia","Spain","Sweden","Switzerland","Turkey","Ukraine",
               "United Kingdom","Vatican City","Liechtenstein","San Marino")

n_america <- c("Antigua and Barbuda","Bahamas","Barbados","Belize","Canada",
               "Costa Rica","Cuba","Dominica","Dominican Republic","El Salvador",
               "Grenada","Guatemala","Haiti","Honduras","Jamaica","Mexico",
               "Nicaragua","Panama","Saint Kitts and Nevis","Saint Lucia",
               "Saint Vincent and the Grenadines","Trinidad and Tobago",
               "United States of America")

s_america <- c("Argentina","Bolivia","Brazil","Chile","Colombia","Ecuador",
               "Guyana","Paraguay","Peru","Suriname","Uruguay","Venezuela")

oceania   <- c("Australia","Fiji Islands","Kiribati","Marshall Islands",
               "Micronesia","New Zealand","Palau","Papua New Guinea","Samoa",
               "Solomon Islands","Tonga","Tuvalu","Vanuatu")

df <- df |>
  mutate(continent = case_when(
    country %in% africa    ~ "Africa",
    country %in% asia      ~ "Asia",
    country %in% europe    ~ "Europe",
    country %in% n_america ~ "North America",
    country %in% s_america ~ "South America",
    country %in% oceania   ~ "Oceania",
    TRUE                   ~ "Other"
  ))

# African subregion lookup
african_subregion <- c(
  "Algeria"="Northern Africa","Angola"="Middle Africa","Benin"="Western Africa",
  "Botswana"="Southern Africa","Burkina Faso"="Western Africa",
  "Burundi"="Eastern Africa","Cameroon"="Middle Africa",
  "Cape Verde"="Western Africa","Central African Republic"="Middle Africa",
  "Chad"="Middle Africa","Comoros"="Eastern Africa","Congo"="Middle Africa",
  "Democratic Republic of Congo"="Middle Africa","Djibouti"="Eastern Africa",
  "Egypt"="Northern Africa","Equatorial Guinea"="Middle Africa",
  "Eritrea"="Eastern Africa","Ethiopia"="Eastern Africa",
  "Gabon"="Middle Africa","Gambia"="Western Africa","Ghana"="Western Africa",
  "Guinea"="Western Africa","Guinea-Bissau"="Western Africa",
  "Kenya"="Eastern Africa","Lesotho"="Southern Africa",
  "Liberia"="Western Africa","Libya"="Northern Africa",
  "Madagascar"="Eastern Africa","Malawi"="Eastern Africa",
  "Mali"="Western Africa","Mauritania"="Western Africa",
  "Mauritius"="Eastern Africa","Morocco"="Northern Africa",
  "Mozambique"="Eastern Africa","Namibia"="Southern Africa",
  "Niger"="Western Africa","Nigeria"="Western Africa",
  "Rwanda"="Eastern Africa","Senegal"="Western Africa",
  "Seychelles Islands"="Eastern Africa","Sierra Leone"="Western Africa",
  "Somalia"="Eastern Africa","South Africa"="Southern Africa",
  "Sudan"="Northern Africa","Swaziland"="Southern Africa",
  "Tanzania"="Eastern Africa","Togo"="Western Africa",
  "Tunisia"="Northern Africa","Uganda"="Eastern Africa",
  "Zambia"="Southern Africa","Zimbabwe"="Southern Africa"
)

df <- df |>
  mutate(subregion = if_else(continent == "Africa",
                             african_subregion[country], NA_character_))

# --- 4. Parse datetime -------------------------------------------------------
cat("Parsing datetime...\n")
df <- df |>
  mutate(last_updated = parse_date_time(last_updated,
                                        orders = c("ymd HMS","ymd HM","ymd"),
                                        quiet  = TRUE)) |>
  filter(!is.na(last_updated))

# --- 5. Feature Engineering --------------------------------------------------
cat("Engineering features...\n")

df <- df |>
  mutate(
    # ── Temporal features ────────────────────────────────────────────────
    month      = month(last_updated, label = TRUE, abbr = TRUE),
    hour       = hour(last_updated),
    day_of_week = wday(last_updated, label = TRUE, abbr = TRUE),
    week_of_year = week(last_updated),

    # ── Season (Southern & Northern Hemisphere aware) ───────────────────
    season = case_when(
      continent %in% c("Africa","Asia","Europe","North America") ~ case_when(
        month(last_updated) %in% c(12, 1, 2) ~ "Winter",
        month(last_updated) %in% c(3, 4, 5)  ~ "Spring",
        month(last_updated) %in% c(6, 7, 8)  ~ "Summer",
        TRUE                                  ~ "Autumn"
      ),
      TRUE ~ case_when(                        # Southern Hemisphere flip
        month(last_updated) %in% c(12, 1, 2) ~ "Summer",
        month(last_updated) %in% c(3, 4, 5)  ~ "Autumn",
        month(last_updated) %in% c(6, 7, 8)  ~ "Winter",
        TRUE                                  ~ "Spring"
      )
    ),

    # ── Heat Index (Rothfusz, valid when T > 27°C & RH > 40%) ───────────
    heat_index = if_else(
      temperature_celsius > 27 & humidity > 40,
      -8.78469475556 +
        1.61139411   * temperature_celsius +
        2.33854883889 * humidity +
        -0.14611605  * temperature_celsius * humidity +
        -0.012308094 * temperature_celsius^2 +
        -0.016424828 * humidity^2 +
        0.002211732  * temperature_celsius^2 * humidity +
        0.00072546   * temperature_celsius   * humidity^2 +
        -0.000003582 * temperature_celsius^2 * humidity^2,
      temperature_celsius  # fallback = actual temp
    ),

    # ── Wind Chill (valid when T < 10°C & wind > 4.8 km/h) ──────────────
    wind_kmh   = wind_mph * 1.60934,
    wind_chill = if_else(
      temperature_celsius < 10 & wind_kmh > 4.8,
      13.12 + 0.6215 * temperature_celsius -
        11.37 * wind_kmh^0.16 +
        0.3965 * temperature_celsius * wind_kmh^0.16,
      temperature_celsius
    ),

    # ── Apparent (feels-like) comfort index ─────────────────────────────
    comfort_index = if_else(
      temperature_celsius > 27 & humidity > 40,
      heat_index,
      if_else(temperature_celsius < 10 & wind_kmh > 4.8,
              wind_chill,
              temperature_celsius)
    ),

    # ── Wind direction cardinal ──────────────────────────────────────────
    wind_cardinal = case_when(
      wind_degree < 22.5  | wind_degree >= 337.5 ~ "N",
      wind_degree < 67.5  ~ "NE",
      wind_degree < 112.5 ~ "E",
      wind_degree < 157.5 ~ "SE",
      wind_degree < 202.5 ~ "S",
      wind_degree < 247.5 ~ "SW",
      wind_degree < 292.5 ~ "W",
      wind_degree < 337.5 ~ "NW",
      TRUE ~ NA_character_
    ),

    # ── Temperature anomaly flag ─────────────────────────────────────────
    is_extreme_temp = temperature_celsius > 40 | temperature_celsius < -10,

    # ── AQI category (US EPA 6-level) ───────────────────────────────────
    aqi_category = case_when(
      air_quality_us_epa_index == 1 ~ "Good",
      air_quality_us_epa_index == 2 ~ "Moderate",
      air_quality_us_epa_index == 3 ~ "Unhealthy for Sensitive",
      air_quality_us_epa_index == 4 ~ "Unhealthy",
      air_quality_us_epa_index == 5 ~ "Very Unhealthy",
      air_quality_us_epa_index == 6 ~ "Hazardous",
      TRUE ~ NA_character_
    ),

    # ── Humidity category ────────────────────────────────────────────────
    humidity_category = case_when(
      humidity < 30  ~ "Dry",
      humidity < 60  ~ "Comfortable",
      humidity < 80  ~ "Humid",
      TRUE           ~ "Very Humid"
    ),

    # ── Precipitation category ───────────────────────────────────────────
    precip_category = case_when(
      precip_mm == 0        ~ "None",
      precip_mm < 2.5       ~ "Light",
      precip_mm < 7.6       ~ "Moderate",
      precip_mm < 50        ~ "Heavy",
      TRUE                  ~ "Extreme"
    )
  ) |>
  select(-wind_kmh)   # drop intermediate column

# --- 6. Cleaning & type enforcement -----------------------------------------
cat("Cleaning and enforcing types...\n")

numeric_cols <- c("temperature_celsius","pressure_mb","humidity","precip_mm",
                  "cloud","wind_degree","wind_mph","air_quality_us_epa_index",
                  "heat_index","wind_chill","comfort_index")

df <- df |>
  mutate(across(all_of(numeric_cols), as.numeric)) |>
  # Cap obvious sensor errors
  mutate(
    humidity             = pmin(pmax(humidity, 0, na.rm = TRUE), 100),
    cloud                = pmin(pmax(cloud, 0, na.rm = TRUE), 100),
    wind_degree          = pmin(pmax(wind_degree, 0, na.rm = TRUE), 360),
    pressure_mb          = if_else(pressure_mb < 800 | pressure_mb > 1100,
                                   NA_real_, pressure_mb)
  ) |>
  # Remove rows missing any core weather variable
  filter(!is.na(temperature_celsius),
         !is.na(humidity),
         !is.na(pressure_mb))

cat(sprintf("  Cleaned rows: %s\n", nrow(df)))

# --- 7. Write to DuckDB ------------------------------------------------------
cat("Writing to DuckDB...\n")

if (file.exists(DB_PATH)) file.remove(DB_PATH)

con <- dbConnect(duckdb(), dbdir = DB_PATH)
dbWriteTable(con, "weather_clean", df, overwrite = TRUE)

# Create helpful indices for fast Shiny queries
dbExecute(con, "CREATE INDEX idx_continent ON weather_clean(continent)")
dbExecute(con, "CREATE INDEX idx_country   ON weather_clean(country)")
dbExecute(con, "CREATE INDEX idx_date      ON weather_clean(last_updated)")

row_count <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM weather_clean")$n
cat(sprintf("  Rows written to DuckDB: %s\n", row_count))

dbDisconnect(con, shutdown = TRUE)
cat("=== build_db.R : Done — database at", DB_PATH, "===\n")
