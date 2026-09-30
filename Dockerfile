FROM python:3.11-slim-bookworm AS dependencies
ENV PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

FROM dependencies AS spark-dependencies
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN pip install --no-cache-dir pyspark==3.5.7
# Resolve Kafka connector dependencies at build time, not during the first demo.
RUN python -c "from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7').getOrCreate(); s.stop()"

FROM dependencies AS app
COPY . /app

FROM spark-dependencies AS spark
COPY . /app
