# ── Shiny App Docker Image ───────────────────────────────────────────────────
# rocker/shiny-verse = Shiny Server + tidyverse + devtools + all system libs
FROM rocker/shiny-verse:4.4.0

# Extra system libraries for the remaining packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    libgit2-dev \
    libudunits2-dev \
    libmagick++-dev \
    cargo \
    && rm -rf /var/lib/apt/lists/*

# Install additional R packages not in the base image
# (tidyverse, lubridate, DBI already present in rocker/shiny-verse)
# TO (install in stages so failures are visible, not silent):
RUN R -e "install.packages(c('shinydashboard','shinycssloaders','plotly','DT'), repos='https://cloud.r-project.org', Ncpus=2)"
RUN R -e "install.packages(c('duckdb','DBI','Rtsne','uwot','igraph'), repos='https://cloud.r-project.org', Ncpus=2)"
RUN R -e "install.packages(c('ggrepel','viridis'), repos='https://cloud.r-project.org', Ncpus=2)"
RUN R -e "install.packages('gifski', repos='https://packagemanager.posit.co/cran/__linux__/bookworm/latest', Ncpus=2)"
RUN R -e "install.packages('gganimate', repos='https://packagemanager.posit.co/cran/__linux__/bookworm/latest', Ncpus=2)"
RUN R -e "install.packages(c('tidymodels','ranger','vip','rsconnect'), repos='https://cloud.r-project.org', Ncpus=2)"

# Copy app source only — data is mounted via docker-compose volume at runtime
WORKDIR /srv/shiny-server/weatherapp
COPY app.R .

# Pre-create mount-point directories so Docker knows they exist
RUN mkdir -p www data/curated

# Shiny Server config — single app, no login
RUN printf 'run_as shiny;\nserver {\n  listen 3838;\n  location / {\n    app_dir /srv/shiny-server/weatherapp;\n    log_dir /var/log/shiny-server;\n  }\n}\n' \
    > /etc/shiny-server/shiny-server.conf

EXPOSE 3838
CMD ["/usr/bin/shiny-server"]
