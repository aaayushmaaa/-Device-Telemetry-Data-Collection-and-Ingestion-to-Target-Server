FROM apache/airflow:2.9.0

USER root
RUN apt-get update && apt-get install -y python3-dev build-essential

USER airflow
RUN pip install --no-cache-dir minio psycopg2-binary
