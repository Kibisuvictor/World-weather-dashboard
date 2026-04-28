# transform/Dockerfile
FROM rocker/r-ver:4.4.0

# System libraries
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install MLflow Python backend (required by R mlflow package)
RUN pip3 install mlflow --quiet

# Install R packages
RUN R -e "install.packages(
  c('tidyverse','lubridate','janitor','duckdb','DBI',
    'tidymodels','ranger','vip','mlflow'),
  repos = 'https://cloud.r-project.org'
)"

WORKDIR /project

COPY build_db.R    .
COPY train_model.R .

# Run both scripts sequentially
CMD Rscript build_db.R && Rscript train_model.R
