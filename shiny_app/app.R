# =============================================================================
# African Weather Dashboard
# Data source: GlobalWeatherRepository (Kaggle)
# Storage: DuckDB (replaces CSV/Parquet loading)
# =============================================================================

# --- 0. Libraries ------------------------------------------------------------
suppressPackageStartupMessages({
  library(shiny)
  library(shinydashboard)
  library(shinycssloaders)
  library(tidyverse)
  library(lubridate)
  library(plotly)
  library(DT)
  library(duckdb)
  library(DBI)
  library(Rtsne)
  library(uwot)
  library(igraph)
  library(gganimate)
  library(ggrepel)
  library(viridis)
  library(gifski)
})

# --- 1. DuckDB Connection & Data Load ----------------------------------------
# Connect to the pre-built DuckDB database (created by transform/build_db.R)
DB_PATH <- "../data/curated/weather.duckdb"

load_weather_data <- function(db_path) {
  if (!file.exists(db_path)) {
    warning("DuckDB file not found at: ", db_path,
            "\nRun transform/build_db.R to generate the database.")
    return(
      tibble(
        country = character(), last_updated = as.POSIXct(character()),
        temperature_celsius = numeric(), pressure_mb = numeric(),
        humidity = numeric(), precip_mm = numeric(), cloud = numeric(),
        wind_degree = numeric(), wind_mph = numeric(),
        wind_direction = character(), location_name = character(),
        air_quality_us_epa_index = numeric(), continent = character(),
        subregion = character(), heat_index = numeric(),
        wind_chill = numeric(), comfort_index = numeric(),
        season = character(), is_extreme_temp = logical(),
        aqi_category = character()
      )
    )
  }
  con <- dbConnect(duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE))
  tbl(con, "weather_clean") |> collect()
}

weather_df <- load_weather_data(DB_PATH)

# Derived globals (safe if df is empty)
available_continents <- sort(unique(weather_df$continent))
if (length(available_continents) == 0) available_continents <- "Africa"

date_range <- if (nrow(weather_df) > 0) {
  list(
    min_year  = min(year(weather_df$last_updated), na.rm = TRUE),
    max_year  = max(year(weather_df$last_updated), na.rm = TRUE),
    min_date  = min(as.Date(weather_df$last_updated), na.rm = TRUE),
    max_date  = max(as.Date(weather_df$last_updated), na.rm = TRUE)
  )
} else {
  list(min_year = 2024, max_year = 2025,
       min_date = Sys.Date() - 365, max_date = Sys.Date())
}

# Feature columns used across multiple tabs
WEATHER_FEATURES <- c("temperature_celsius", "pressure_mb", "humidity",
                       "precip_mm", "cloud", "wind_degree", "wind_mph")

# =============================================================================
# --- 2. UI -------------------------------------------------------------------
# =============================================================================
ui <- dashboardPage(
  skin = "blue",

  dashboardHeader(title = "African Weather Analytics"),

  # --- Sidebar ---------------------------------------------------------------
  dashboardSidebar(
    sidebarMenu(
      menuItem("Overview",             tabName = "tab_overview",  icon = icon("tachometer-alt")),
      menuItem("High-Dim Analysis",    tabName = "tab_dimred",    icon = icon("sitemap")),
      menuItem("Similarity Network",   tabName = "tab_network",   icon = icon("project-diagram")),
      menuItem("Temporal Trends",      tabName = "tab_temporal",  icon = icon("chart-line")),
      menuItem("EA Network Case Study",tabName = "tab_ea",        icon = icon("microscope")),
      menuItem("Data Explorer",        tabName = "tab_explorer",  icon = icon("table")),
      menuItem("ML Insights",          tabName = "tab_ml",        icon = icon("robot"))
    ),
    hr(),
    h5("Global Filters", style = "padding-left:15px; color:#aaa;"),
    selectInput("sel_continent", "Continent:",
                choices = available_continents, selected = "Africa"),
    numericInput("sel_year", "Year:",
                 value  = date_range$max_year,
                 min    = date_range$min_year,
                 max    = date_range$max_year,
                 step   = 1),
    uiOutput("ui_subregion"),
    uiOutput("ui_country")
  ),

  # --- Body ------------------------------------------------------------------
  dashboardBody(
    tags$head(tags$style(HTML("
      .content-wrapper { overflow: auto; }
      .info-box-icon { font-size: 28px; line-height: 60px; }
    "))),

    tabItems(

      # ── Overview Tab ────────────────────────────────────────────────────────
      tabItem(tabName = "tab_overview",
        fluidRow(
          valueBoxOutput("vbox_countries",  width = 3),
          valueBoxOutput("vbox_avg_temp",   width = 3),
          valueBoxOutput("vbox_avg_humid",  width = 3),
          valueBoxOutput("vbox_obs",        width = 3)
        ),
        fluidRow(
          box(width = 12, title = "Average Weather Conditions Heatmap",
              status = "info", solidHeader = TRUE, collapsible = TRUE,
              withSpinner(plotOutput("plot_heatmap", height = "350px"), type = 6))
        ),
        fluidRow(
          box(width = 6, title = "Top 20 Countries by Avg Temperature",
              status = "info", solidHeader = TRUE, collapsible = TRUE,
              withSpinner(plotlyOutput("plot_country_bar"), type = 6)),
          box(width = 6, title = "Observations by Subregion",
              status = "info", solidHeader = TRUE, collapsible = TRUE,
              withSpinner(plotlyOutput("plot_subregion_bar"), type = 6))
        ),
        fluidRow(
          box(width = 6, title = "Temperature Distribution by Subregion",
              status = "warning", solidHeader = TRUE, collapsible = TRUE,
              withSpinner(plotlyOutput("plot_temp_dist"), type = 6)),
          box(width = 6, title = "Air Quality Index Distribution",
              status = "warning", solidHeader = TRUE, collapsible = TRUE,
              withSpinner(plotlyOutput("plot_aqi_dist"), type = 6))
        )
      ),

      # ── High-Dim Analysis Tab ───────────────────────────────────────────────
      tabItem(tabName = "tab_dimred",
        fluidRow(
          box(width = 12, title = "Dimensionality Reduction Controls",
              status = "primary", solidHeader = TRUE, collapsible = TRUE,
              fluidRow(
                column(3, selectInput("dimred_method", "Method:",
                                      choices = c("UMAP", "t-SNE"), selected = "UMAP")),
                column(3, selectInput("dimred_color", "Color Points By:",
                                      choices = c("air_quality_us_epa_index","subregion","country","aqi_category","season"),
                                      selected = "aqi_category")),
                column(3, conditionalPanel(
                  "input.dimred_method == 't-SNE'",
                  sliderInput("dimred_perplexity", "Perplexity:", min = 2, max = 50, value = 30)
                )),
                column(3, br(), actionButton("btn_dimred", "Run Analysis",
                                              icon = icon("play"), class = "btn-success btn-block"))
              ))
        ),
        fluidRow(
          box(width = 12, title = "Dimensionality Reduction Plot",
              status = "info", solidHeader = TRUE,
              withSpinner(plotlyOutput("plot_dimred", height = "600px"), type = 6))
        )
      ),

      # ── Similarity Network Tab ──────────────────────────────────────────────
      tabItem(tabName = "tab_network",
        fluidRow(
          box(width = 12, title = "Network Controls",
              status = "primary", solidHeader = TRUE, collapsible = TRUE,
              fluidRow(
                column(4, sliderInput("net_threshold", "Similarity Threshold:",
                                      min = 0.05, max = 0.95, value = 0.5, step = 0.05)),
                column(4, selectInput("net_layout", "Layout:",
                                      choices = c("Fruchterman-Reingold" = "layout_with_fr",
                                                  "Kamada-Kawai"         = "layout_with_kk",
                                                  "Circle"               = "layout_in_circle"),
                                      selected = "layout_with_fr")),
                column(4, br(), checkboxInput("net_communities",
                                              "Show Communities (Louvain)", value = TRUE))
              ))
        ),
        fluidRow(
          box(width = 12, title = "Weather Similarity Network",
              status = "info", solidHeader = TRUE,
              withSpinner(plotOutput("plot_network", height = "700px"), type = 6))
        )
      ),

      # ── Temporal Trends Tab ─────────────────────────────────────────────────
      tabItem(tabName = "tab_temporal",
        fluidRow(
          box(width = 12, title = "Animation Controls",
              status = "primary", solidHeader = TRUE, collapsible = TRUE,
              fluidRow(
                column(3, selectInput("anim_x", "X-axis:",
                                      choices = WEATHER_FEATURES,
                                      selected = "temperature_celsius")),
                column(3, selectInput("anim_y", "Y-axis:",
                                      choices = WEATHER_FEATURES,
                                      selected = "humidity")),
                column(3, selectInput("anim_size", "Bubble Size:",
                                      choices = WEATHER_FEATURES,
                                      selected = "precip_mm")),
                column(3, sliderInput("anim_fps", "Speed (FPS):",
                                      min = 1, max = 10, value = 3))
              ),
              actionButton("btn_animate", "Generate Animation",
                           icon = icon("film"), class = "btn-success"))
        ),
        fluidRow(
          box(width = 12, title = "Temporal Trends Animation",
              status = "info", solidHeader = TRUE,
              uiOutput("ui_animation"))
        )
      ),

      # ── EA Network Case Study Tab ───────────────────────────────────────────
      tabItem(tabName = "tab_ea",
        h3("Eastern Africa Capitals: Weather Similarity Network",
           style = "padding-left:15px;"),
        p("Fixed case study — Eastern Africa on April 1st, 2025.",
          style = "padding-left:15px; color:#666;"),
        fluidRow(
          box(title = "Modularity Groups",      width = 6, status = "danger", solidHeader = TRUE,
              withSpinner(plotOutput("plot_ea_mod"), type = 6)),
          box(title = "Edge Betweenness Groups", width = 6, status = "danger", solidHeader = TRUE,
              withSpinner(plotOutput("plot_ea_btw"), type = 6))
        ),
        fluidRow(
          box(title = "Centrality Measures", width = 12, status = "danger", solidHeader = TRUE,
              withSpinner(DTOutput("tbl_ea_centrality"), type = 6))
        )
      ),

      # ── Data Explorer Tab ───────────────────────────────────────────────────
      tabItem(tabName = "tab_explorer",
        fluidRow(
          box(width = 12, title = "Explorer Filters",
              status = "primary", solidHeader = TRUE, collapsible = TRUE,
              fluidRow(
                column(4, uiOutput("ui_explorer_subregion")),
                column(4, selectInput("exp_country", "Country:",
                                      choices = c("All"), selected = "All")),
                column(4, dateRangeInput("exp_dates", "Date Range:",
                                         start = date_range$min_date,
                                         end   = date_range$max_date,
                                         min   = date_range$min_date,
                                         max   = date_range$max_date))
              ))
        ),
        fluidRow(
          box(width = 12, title = "Weather Data Explorer",
              status = "info", solidHeader = TRUE,
              withSpinner(DTOutput("tbl_explorer"), type = 6))
        )
      ),

      # ── ML Insights Tab ─────────────────────────────────────────────────────
      tabItem(tabName = "tab_ml",
        h3("Machine Learning Insights", style = "padding-left:15px;"),
        p("Model predictions loaded from the MLflow/RDS model registry.",
          style = "padding-left:15px; color:#666;"),
        fluidRow(
          box(width = 6, title = "Predicted vs Actual Temperature",
              status = "primary", solidHeader = TRUE, collapsible = TRUE,
              withSpinner(plotlyOutput("plot_ml_pred"), type = 6)),
          box(width = 6, title = "Feature Importance",
              status = "primary", solidHeader = TRUE, collapsible = TRUE,
              withSpinner(plotlyOutput("plot_ml_importance"), type = 6))
        ),
        fluidRow(
          box(width = 12, title = "Model Metrics",
              status = "info", solidHeader = TRUE, collapsible = TRUE,
              verbatimTextOutput("txt_ml_metrics"))
        )
      )
    )
  )
)

# =============================================================================
# --- 3. Server ---------------------------------------------------------------
# =============================================================================
server <- function(input, output, session) {

  # ── A. Dynamic Sidebar UI --------------------------------------------------
  output$ui_subregion <- renderUI({
    req(input$sel_continent)
    if (input$sel_continent == "Africa") {
      sub_choices <- c("All", sort(unique(na.omit(
        weather_df$subregion[weather_df$continent == "Africa"]
      ))))
      selectInput("sel_subregion", "African Subregion:",
                  choices = sub_choices, selected = "All")
    }
  })

  output$ui_country <- renderUI({
    req(input$sel_continent)
    df_c <- weather_df |> filter(continent == input$sel_continent)
    if (!is.null(input$sel_subregion) &&
        input$sel_subregion != "All" &&
        input$sel_continent == "Africa") {
      df_c <- df_c |> filter(subregion == input$sel_subregion)
    }
    choices <- c("All", sort(unique(na.omit(df_c$country))))
    selectInput("sel_country", "Country:", choices = choices, selected = "All")
  })

  # ── B. Core Filtered Reactive ──────────────────────────────────────────────
  filtered_df <- reactive({
    req(input$sel_continent, input$sel_year)

    df <- weather_df |>
      filter(continent == input$sel_continent,
             year(last_updated) == input$sel_year)

    if (!is.null(input$sel_subregion) &&
        input$sel_subregion != "All" &&
        input$sel_continent == "Africa") {
      df <- df |> filter(subregion == input$sel_subregion)
    }
    if (!is.null(input$sel_country) && input$sel_country != "All") {
      df <- df |> filter(country == input$sel_country)
    }

    validate(need(nrow(df) > 0,
                  "No data for the selected filters. Try changing the year or continent."))
    df
  })

  # ── C. Overview Value Boxes ────────────────────────────────────────────────
  output$vbox_countries <- renderValueBox({
    valueBox(
      n_distinct(filtered_df()$country),
      "Countries", icon = icon("globe-africa"), color = "blue"
    )
  })
  output$vbox_avg_temp <- renderValueBox({
    valueBox(
      paste0(round(mean(filtered_df()$temperature_celsius, na.rm = TRUE), 1), "°C"),
      "Avg Temperature", icon = icon("thermometer-half"), color = "orange"
    )
  })
  output$vbox_avg_humid <- renderValueBox({
    valueBox(
      paste0(round(mean(filtered_df()$humidity, na.rm = TRUE), 1), "%"),
      "Avg Humidity", icon = icon("tint"), color = "aqua"
    )
  })
  output$vbox_obs <- renderValueBox({
    valueBox(
      format(nrow(filtered_df()), big.mark = ","),
      "Observations", icon = icon("database"), color = "green"
    )
  })

  # ── D. Overview Plots ──────────────────────────────────────────────────────
  output$plot_heatmap <- renderPlot({
    df <- filtered_df()
    grp_var <- if (input$sel_continent == "Africa" && "subregion" %in% names(df)) {
      sym("subregion")
    } else {
      sym("country")
    }

    summary_data <- df |>
      filter(!is.na(!!grp_var)) |>
      group_by(!!grp_var) |>
      summarise(
        Temperature  = mean(temperature_celsius,     na.rm = TRUE),
        Humidity     = mean(humidity,                na.rm = TRUE),
        Cloud        = mean(cloud,                   na.rm = TRUE),
        Precip_mm    = mean(precip_mm,               na.rm = TRUE),
        Wind_MPH     = mean(wind_mph,                na.rm = TRUE),
        AQI          = mean(air_quality_us_epa_index,na.rm = TRUE),
        .groups = "drop"
      ) |>
      pivot_longer(-!!grp_var, names_to = "metric", values_to = "value") |>
      filter(!is.na(value))

    validate(need(nrow(summary_data) > 0, "Not enough data for heatmap."))

    ggplot(summary_data, aes(x = metric, y = !!grp_var, fill = value)) +
      geom_tile(color = "white", lwd = 0.5) +
      geom_text(aes(label = round(value, 1)), size = 3.5) +
      scale_fill_viridis_c(option = "turbo", direction = -1) +
      labs(
        title = paste("Weather Conditions Heatmap —", input$sel_continent, input$sel_year),
        x = NULL, y = NULL, fill = "Avg Value"
      ) +
      theme_minimal(base_size = 12) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1),
            plot.title  = element_text(hjust = 0.5, face = "bold"),
            panel.grid  = element_blank())
  })

  output$plot_country_bar <- renderPlotly({
    df <- filtered_df() |>
      filter(!is.na(country), !is.na(temperature_celsius)) |>
      group_by(country) |>
      summarise(avg_temp = mean(temperature_celsius, na.rm = TRUE),
                n = n(), .groups = "drop") |>
      arrange(desc(avg_temp)) |>
      head(20)

    validate(need(nrow(df) > 0, "No country temperature data."))

    plot_ly(df,
            x = ~reorder(country, -avg_temp), y = ~avg_temp,
            type = "bar",
            text = ~paste0(country, "<br>Avg: ", round(avg_temp, 1), "°C<br>n = ", n),
            hoverinfo = "text",
            marker = list(color = "rgba(255,127,80,0.85)",
                          line  = list(color = "rgba(255,80,40,1)", width = 1))) |>
      layout(xaxis  = list(title = "", tickangle = -45),
             yaxis  = list(title = "Avg Temp (°C)"),
             margin = list(b = 120, t = 40))
  })

  output$plot_subregion_bar <- renderPlotly({
    df <- filtered_df() |>
      filter(!is.na(subregion)) |>
      count(subregion, sort = TRUE)

    validate(need(nrow(df) > 0, "No subregion data for bar chart."))

    plot_ly(df, x = ~reorder(subregion, n), y = ~n, type = "bar",
            marker = list(color = "rgba(55,138,220,0.85)")) |>
      layout(xaxis  = list(title = ""),
             yaxis  = list(title = "Observations"),
             margin = list(b = 100))
  })

  output$plot_temp_dist <- renderPlotly({
    df <- filtered_df() |> filter(!is.na(temperature_celsius), !is.na(subregion))
    validate(need(nrow(df) > 0, "No temperature distribution data."))

    plot_ly(df, x = ~temperature_celsius, color = ~subregion,
            type = "histogram", opacity = 0.7, nbinsx = 30) |>
      layout(barmode = "overlay",
             xaxis = list(title = "Temperature (°C)"),
             yaxis = list(title = "Count"),
             legend = list(title = list(text = "Subregion")))
  })

  output$plot_aqi_dist <- renderPlotly({
    df <- filtered_df() |>
      filter(!is.na(aqi_category)) |>
      count(aqi_category) |>
      mutate(aqi_category = factor(aqi_category,
                                   levels = c("Good","Moderate","Unhealthy for Sensitive",
                                              "Unhealthy","Very Unhealthy","Hazardous")))

    validate(need(nrow(df) > 0, "No AQI data."))

    aqi_colors <- c("Good"                    = "#00e400",
                    "Moderate"                = "#ffff00",
                    "Unhealthy for Sensitive" = "#ff7e00",
                    "Unhealthy"               = "#ff0000",
                    "Very Unhealthy"          = "#8f3f97",
                    "Hazardous"               = "#7e0023")

    plot_ly(df, x = ~aqi_category, y = ~n, type = "bar",
            marker = list(color = aqi_colors[as.character(df$aqi_category)])) |>
      layout(xaxis = list(title = "AQI Category"),
             yaxis = list(title = "Count"),
             margin = list(b = 100))
  })

  # ── E. High-Dim Analysis ───────────────────────────────────────────────────
  dimred_data <- eventReactive(input$btn_dimred, {
    df <- filtered_df()
    color_var <- input$dimred_color

    missing <- setdiff(WEATHER_FEATURES, names(df))
    validate(need(length(missing) == 0,
                  paste("Missing columns:", paste(missing, collapse = ", "))))
    validate(need(color_var %in% names(df),
                  paste("Color variable", color_var, "not in data.")))

    data_dr <- df |>
      select(all_of(c(WEATHER_FEATURES, color_var))) |>
      drop_na()

    validate(need(nrow(data_dr) > 5, "Not enough data (>5 rows) for dimensionality reduction."))

    scaled  <- scale(data_dr |> select(all_of(WEATHER_FEATURES)))
    col_vals <- data_dr[[color_var]]

    if (input$dimred_method == "t-SNE") {
      n    <- nrow(scaled)
      perp <- min(input$dimred_perplexity, max(1, floor((n - 1) / 3) - 1))
      set.seed(42)
      coords <- Rtsne(scaled, perplexity = perp,
                      check_duplicates = FALSE)$Y
    } else {
      set.seed(42)
      coords <- umap(scaled)
    }

    list(coords = coords, color = col_vals,
         method = input$dimred_method, color_by = color_var)
  })

  output$plot_dimred <- renderPlotly({
    d <- dimred_data()
    df_p <- tibble(X1 = d$coords[, 1], X2 = d$coords[, 2], Color = d$color)

    base <- list(
      data = df_p, x = ~X1, y = ~X2,
      type = "scatter", mode = "markers",
      text = ~paste0(gsub("_", " ", d$color_by), ": ", Color),
      hoverinfo = "text"
    )

    if (is.numeric(df_p$Color)) {
      base$color  <- ~Color
      base$marker <- list(colorscale = "Viridis", showscale = TRUE)
    } else {
      base$color <- ~as.factor(Color)
    }

    do.call(plot_ly, base) |>
      layout(
        xaxis  = list(title = paste(d$method, "Dim 1")),
        yaxis  = list(title = paste(d$method, "Dim 2")),
        legend = list(title = list(text = gsub("_", " ", d$color_by)))
      )
  })

  # ── F. Weather Similarity Network ──────────────────────────────────────────
  network_graph <- reactive({
    df <- filtered_df()
    validate(need("location_name" %in% names(df),
                  "Column 'location_name' required for network."))

    sim_data <- df |>
      select(location_name, all_of(WEATHER_FEATURES)) |>
      group_by(location_name) |>
      summarise(across(all_of(WEATHER_FEATURES), ~mean(., na.rm = TRUE)),
                .groups = "drop") |>
      drop_na()

    validate(need(nrow(sim_data) >= 2,
                  "Need at least 2 locations for network analysis."))

    scaled <- scale(sim_data[, WEATHER_FEATURES])
    rownames(scaled) <- sim_data$location_name

    sim_mat <- 1 / (1 + as.matrix(dist(scaled, method = "euclidean")))
    edges   <- which(sim_mat > input$net_threshold & upper.tri(sim_mat),
                     arr.ind = TRUE)

    if (nrow(edges) == 0) {
      g <- make_empty_graph(n = nrow(sim_data), directed = FALSE)
      V(g)$name  <- sim_data$location_name
      V(g)$label <- V(g)$name
      return(g)
    }

    edges_df <- data.frame(
      from   = rownames(sim_mat)[edges[, 1]],
      to     = colnames(sim_mat)[edges[, 2]],
      weight = sim_mat[edges]
    )

    g <- graph_from_data_frame(edges_df,
                               vertices = sim_data |> select(location_name),
                               directed = FALSE)
    V(g)$label <- V(g)$name
    g
  })

  output$plot_network <- renderPlot({
    g <- network_graph()
    validate(need(length(V(g)) > 0, "No nodes to plot."))

    layout_fn <- get(input$net_layout)

    if (length(E(g)) == 0) {
      plot(g, layout = layout_fn, vertex.size = 8,
           vertex.color = "skyblue", vertex.label = V(g)$label,
           main = paste("Similarity Network (threshold:", input$net_threshold, ") — No edges"))
      return()
    }

    if (input$net_communities) {
      comms  <- cluster_louvain(g)
      n_comm <- max(comms$membership)
      cols   <- viridis::viridis_pal(option = "D")(n_comm)
      V(g)$color <- cols[comms$membership]
    } else {
      V(g)$color <- "skyblue"
    }

    plot(g,
         layout        = layout_fn(g),
         vertex.color  = V(g)$color,
         vertex.size   = 7,
         vertex.label  = V(g)$label,
         vertex.label.cex = 0.85,
         edge.width    = E(g)$weight * 4,
         edge.color    = "gray70",
         main          = paste("Similarity Network — Threshold:", input$net_threshold))

    if (input$net_communities && length(E(g)) > 0) {
      comms <- cluster_louvain(g)
      n_comm <- max(comms$membership)
      cols   <- viridis::viridis_pal(option = "D")(n_comm)
      if (n_comm <= 12) {
        legend("topright",
               legend = paste("Community", seq_len(n_comm)),
               fill   = cols, cex = 0.75, bty = "n")
      }
    }
  }, res = 96)

  # ── G. Temporal Animation ──────────────────────────────────────────────────
  anim_path <- reactiveVal(NULL)

  observeEvent(input$btn_animate, {
    anim_path(NULL)
    showModal(modalDialog("Generating animation — this may take 30–60 seconds…",
                          footer = NULL, easyClose = FALSE))

    df_anim <- tryCatch({
      filtered_df() |>
        mutate(yr_month = floor_date(last_updated, "month") |> as.Date()) |>
        group_by(yr_month, country, subregion) |>
        summarise(
          across(all_of(WEATHER_FEATURES), ~mean(., na.rm = TRUE)),
          subregion = first(subregion),
          .groups = "drop"
        ) |>
        drop_na()
    }, error = function(e) NULL)

    validate(need(!is.null(df_anim) && nrow(df_anim) > 0,
                  "No data for animation."))

    if (!dir.exists("www")) dir.create("www")
    fname <- paste0("anim_", format(Sys.time(), "%Y%m%d%H%M%S"), ".gif")
    fpath <- file.path("www", fname)

    p <- ggplot(df_anim,
                aes(x     = .data[[input$anim_x]],
                    y     = .data[[input$anim_y]],
                    size  = .data[[input$anim_size]],
                    color = subregion)) +
      geom_point(alpha = 0.7) +
      geom_text_repel(aes(label = country), size = 3, max.overlaps = 12,
                      segment.alpha = 0.4) +
      scale_color_viridis_d(option = "turbo") +
      scale_size_continuous(range = c(3, 14)) +
      labs(
        title    = paste("Monthly Trends —", input$sel_continent, input$sel_year),
        subtitle = "Month: {frame_time}",
        x        = str_to_title(gsub("_", " ", input$anim_x)),
        y        = str_to_title(gsub("_", " ", input$anim_y)),
        color    = "Subregion",
        size     = str_to_title(gsub("_", " ", input$anim_size))
      ) +
      theme_minimal(base_size = 13) +
      theme(plot.title    = element_text(face = "bold", hjust = 0.5),
            plot.subtitle = element_text(hjust = 0.5),
            legend.position = "bottom") +
      transition_time(yr_month) +
      ease_aes("linear") +
      enter_fade() + exit_fade()

    n_frames <- length(unique(df_anim$yr_month))

    tryCatch({
      anim_save(fpath,
                animate(p,
                        nframes  = max(n_frames, 10),
                        fps      = input$anim_fps,
                        duration = max(5, n_frames / input$anim_fps),
                        width    = 960, height = 720, res = 96,
                        renderer = gifski_renderer()))
      anim_path(fname)
    }, error = function(e) {
      showNotification(paste("Animation error:", e$message), type = "error", duration = 15)
      anim_path("ERROR")
    })
    removeModal()
  })

  output$ui_animation <- renderUI({
    p <- anim_path()
    if (is.null(p)) {
      tags$p("Click 'Generate Animation' to start.",
             style = "text-align:center; padding:30px; color:#888;")
    } else if (p == "ERROR") {
      tags$p("Animation failed. Check filters and data availability.",
             style = "color:red; text-align:center; padding:30px;")
    } else {
      tags$img(src   = paste0(p, "?t=", as.numeric(Sys.time())),
               alt   = "Temporal animation",
               style = "max-width:100%; display:block; margin:auto;")
    }
  })

  # ── H. EA Network Case Study ───────────────────────────────────────────────
  ea_data <- reactive({
    ea_raw <- weather_df |>
      filter(continent == "Africa",
             year(last_updated) == 2025,
             as.Date(last_updated) == as.Date("2025-04-01"),
             subregion == "Eastern Africa")

    sim_ea <- ea_raw |>
      select(location_name, all_of(WEATHER_FEATURES)) |>
      distinct(location_name, .keep_all = TRUE) |>
      drop_na()

    validate(need(nrow(sim_ea) >= 2,
                  "Not enough Eastern Africa data for April 1 2025."))

    scaled_ea <- scale(sim_ea[, WEATHER_FEATURES])
    rownames(scaled_ea) <- sim_ea$location_name

    dist_m   <- as.matrix(dist(scaled_ea))
    thresh   <- mean(dist_m)
    edge_idx <- which(dist_m <= thresh & lower.tri(dist_m), arr.ind = TRUE)

    if (nrow(edge_idx) == 0) {
      g <- make_empty_graph(n = nrow(sim_ea), directed = FALSE)
      V(g)$name <- sim_ea$location_name
    } else {
      el <- matrix(c(rownames(dist_m)[edge_idx[, 1]],
                     colnames(dist_m)[edge_idx[, 2]]),
                   ncol = 2)
      g <- graph_from_edgelist(el, directed = FALSE)
    }
    V(g)$label <- V(g)$name

    cent <- tibble(
      label       = V(g)$name,
      degree      = centr_degree(g, mode = "all")$res,
      eigen       = if (length(E(g)) > 0) eigen_centrality(g)$vector
                    else rep(0, length(V(g))),
      closeness   = igraph::closeness(g, normalized = TRUE),
      betweenness = igraph::betweenness(g, normalized = TRUE)
    ) |> arrange(desc(degree))

    list(graph = g, centrality = cent)
  })

  make_ea_plot <- function(g, community_fn, layout_fn, title) {
    validate(need(length(V(g)) > 0, paste("Empty graph for", title)))
    membership <- if (length(E(g)) > 0) community_fn(g)$membership
                  else rep(1, length(V(g)))
    deg <- centr_degree(g, mode = "all")$res
    plot(g,
         vertex.color = membership,
         vertex.size  = 5 + deg / max(1, max(deg, na.rm = TRUE)) * 8,
         vertex.label = V(g)$label,
         vertex.label.cex = 0.9,
         edge.color   = "gray60",
         layout       = layout_fn(g),
         main         = title)
  }

  output$plot_ea_mod <- renderPlot({
    d <- ea_data()
    make_ea_plot(d$graph, cluster_fast_greedy, layout_with_fr,
                 "EA Network — Modularity Groups (Apr 1, 2025)")
  }, res = 96)

  output$plot_ea_btw <- renderPlot({
    d <- ea_data()
    make_ea_plot(d$graph, cluster_edge_betweenness, layout_with_kk,
                 "EA Network — Edge Betweenness Groups (Apr 1, 2025)")
  }, res = 96)

  output$tbl_ea_centrality <- renderDT({
    d <- ea_data()
    validate(need(nrow(d$centrality) > 0, "No centrality data."))
    datatable(d$centrality,
              options = list(pageLength = 10, scrollX = TRUE, autoWidth = TRUE),
              rownames = FALSE) |>
      formatRound(c("eigen", "closeness", "betweenness"), digits = 4)
  })

  # ── I. Data Explorer ───────────────────────────────────────────────────────
  output$ui_explorer_subregion <- renderUI({
    if (input$sel_continent == "Africa") {
      choices <- c("All", sort(unique(na.omit(filtered_df()$subregion))))
      selectInput("exp_subregion", "Subregion:",
                  choices = choices, selected = "All")
    }
  })

  observe({
    df_c <- filtered_df()
    if (!is.null(input$exp_subregion) && input$exp_subregion != "All" &&
        input$sel_continent == "Africa") {
      df_c <- df_c |> filter(subregion == input$exp_subregion)
    }
    updateSelectInput(session, "exp_country",
                      choices  = c("All", sort(unique(na.omit(df_c$country)))),
                      selected = "All")
  })

  explorer_df <- reactive({
    df <- filtered_df()
    if (!is.null(input$exp_subregion) && input$exp_subregion != "All" &&
        input$sel_continent == "Africa") {
      df <- df |> filter(subregion == input$exp_subregion)
    }
    if (!is.null(input$exp_country) && input$exp_country != "All") {
      df <- df |> filter(country == input$exp_country)
    }
    if (!is.null(input$exp_dates)) {
      df <- df |>
        filter(as.Date(last_updated) >= input$exp_dates[1],
               as.Date(last_updated) <= input$exp_dates[2])
    }
    validate(need(nrow(df) > 0, "No data for the current explorer filters."))
    df |> select(-any_of(c("continent_code", "country_code")))
  })

  output$tbl_explorer <- renderDT({
    datatable(
      explorer_df(),
      options = list(pageLength = 15, scrollX = TRUE, autoWidth = TRUE,
                     search = list(regex = TRUE, caseInsensitive = TRUE)),
      filter   = "top",
      rownames = FALSE,
      class    = "display compact"
    )
  })

  # ── J. ML Insights ─────────────────────────────────────────────────────────
  ml_results <- reactive({
    model_path  <- "../data/curated/weather_model.rds"
    metrics_path <- "../data/curated/model_metrics.rds"

    if (!file.exists(model_path)) {
      return(NULL)
    }
    list(
      model   = readRDS(model_path),
      metrics = if (file.exists(metrics_path)) readRDS(metrics_path) else NULL
    )
  })

  output$plot_ml_pred <- renderPlotly({
    res <- ml_results()
    validate(need(!is.null(res), "Run transform/train_model.R to generate ML outputs."))

    df   <- filtered_df() |>
      select(all_of(WEATHER_FEATURES), country, subregion) |>
      drop_na()

    preds <- tryCatch(
      predict(res$model, new_data = df)$.pred,
      error = function(e) NULL
    )

    validate(need(!is.null(preds), "Could not generate predictions with current data."))

    df_p <- tibble(actual = df$temperature_celsius, predicted = preds)

    plot_ly(df_p, x = ~actual, y = ~predicted, type = "scatter",
            mode = "markers", opacity = 0.5,
            marker = list(color = "rgba(55,138,220,0.5)")) |>
      add_lines(x = range(df_p$actual, na.rm = TRUE),
                y = range(df_p$actual, na.rm = TRUE),
                line = list(color = "red", dash = "dash"),
                name = "Perfect fit") |>
      layout(xaxis = list(title = "Actual Temperature (°C)"),
             yaxis = list(title = "Predicted Temperature (°C)"))
  })

  output$plot_ml_importance <- renderPlotly({
    res <- ml_results()
    validate(need(!is.null(res), "No model loaded."))

    imp <- tryCatch(
      vip::vi(res$model) |> head(10),
      error = function(e) NULL
    )
    validate(need(!is.null(imp), "Variable importance not available for this model type."))

    plot_ly(imp, x = ~Importance, y = ~reorder(Variable, Importance),
            type = "bar", orientation = "h",
            marker = list(color = "rgba(29,158,117,0.8)")) |>
      layout(xaxis = list(title = "Importance"),
             yaxis = list(title = ""))
  })

  output$txt_ml_metrics <- renderPrint({
    res <- ml_results()
    if (is.null(res) || is.null(res$metrics)) {
      cat("No model metrics found. Run transform/train_model.R.\n")
    } else {
      print(res$metrics)
    }
  })

  # ── Cleanup ────────────────────────────────────────────────────────────────
  session$onSessionEnded(function() {
    gifs <- list.files("www", pattern = "^anim_.*\\.gif$", full.names = TRUE)
    if (length(gifs) > 0) unlink(gifs)
  })
}

# =============================================================================
shinyApp(ui, server)
