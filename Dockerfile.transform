# Imagen de transformación (dbt + Spark) que corre Kestra en pipeline_transform.
#   docker build -f Dockerfile.transform -t pset2-transform:1.0 .
FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir "dbt-snowflake~=1.9.0" pyspark==3.5.3 pandas==2.2.2 pyarrow==16.1.0 \
    "snowflake-connector-python[pandas]" xlrd requests
ENV SPARK_LOCAL_IP=127.0.0.1
WORKDIR /work
