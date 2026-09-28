FROM python:3.11-slim-bookworm
USER root
WORKDIR /app

# Java is required by Spark; pip-installed PySpark provides spark-submit.
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyspark==3.5.1 psycopg2-binary==2.9.9

COPY common/ /app/common/
COPY spark_jobs/ /app/spark_jobs/
ENV PYTHONUNBUFFERED=1

# Kafka + Postgres JDBC connectors are downloaded by spark-submit --packages
# when the container starts (needs internet the first time).
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3", \
     "/app/spark_jobs/streaming_job.py"]
