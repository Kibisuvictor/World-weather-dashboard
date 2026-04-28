# ── Shiny App Docker Image ───────────────────────────────────────────────────
FROM rocker/shiny:4.4.0

# System libraries needed by R packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libgdal-dev \
    libfontconfig1-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    libudunits2-dev \
    libgit2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install R packages (pinned via renv — see renv.lock)
COPY renv.lock renv.lock
RUN R -e "install.packages('renv', repos='https://cloud.r-project.org')" && \
    R -e "renv::restore()"

# Copy app files
WORKDIR /srv/shiny-server/weatherapp
COPY app.R .
COPY www/   www/

# Copy curated DuckDB (built in CI before this step)
COPY ../data/curated/ /srv/shiny-server/weatherapp/../data/curated/

# Shiny Server config — disable login, single app mode
RUN echo 'run_as shiny;\n\
server {\n\
  listen 3838;\n\
  location / {\n\
    app_dir /srv/shiny-server/weatherapp;\n\
    log_dir /var/log/shiny-server;\n\
  }\n\
}' > /etc/shiny-server/shiny-server.conf

EXPOSE 3838
CMD ["/usr/bin/shiny-server"]
