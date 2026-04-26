FROM apache/airflow:2.10.2-python3.10

USER root

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Install requirements
# Using constraints ensures we don't break the airflow installation
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
