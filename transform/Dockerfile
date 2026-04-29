# transform/Dockerfile
# rocker/tidyverse already includes R, tidyverse, devtools + all system libs
FROM rocker/tidyverse:4.4.0

# Extra system libraries needed by the additional R packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    python3-venv \
    python3-full \
    libxml2-dev \
    libgit2-dev \
    && rm -rf /var/lib/apt/lists/*

# Install MLflow into a venv so the binary path is predictable
RUN python3 -m venv /opt/mlflow-env && \
    /opt/mlflow-env/bin/pip install --quiet "mlflow==2.3.2"

# Tell the R mlflow package exactly where to find the binaries
ENV MLFLOW_PYTHON_BIN=/opt/mlflow-env/bin/python3
ENV MLFLOW_BIN=/opt/mlflow-env/bin/mlflow

# Verify the binary exists at build time — fails the build early if not
RUN /opt/mlflow-env/bin/mlflow --version

# Install remaining R packages
# tidyverse & lubridate are already in the base image — skip them
RUN R -e " \
  pkgs <- c('janitor','duckdb','DBI', \
            'tidymodels','ranger','vip','mlflow'); \
  install.packages(pkgs, repos='https://cloud.r-project.org', \
                   Ncpus=2, quiet=TRUE) \
"

WORKDIR /project

COPY build_db.R    .
COPY train_model.R .

# Run both scripts sequentially; exit non-zero if either fails
CMD Rscript build_db.R && Rscript train_model.R
