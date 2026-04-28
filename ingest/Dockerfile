FROM python:3.11-slim

WORKDIR /project

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ingest script
COPY ingest.py .

# Kaggle credentials are passed as env vars at runtime:
#   -e KAGGLE_USERNAME=xxx -e KAGGLE_KEY=yyy
ENV KAGGLE_CONFIG_DIR=/root/.kaggle

CMD ["python", "ingest.py"]
