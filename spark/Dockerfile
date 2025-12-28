FROM apache/spark:3.5.1

USER root

RUN apt-get update && apt-get install -y curl

# PostgreSQL JDBC driver
RUN curl -L -o /opt/spark/jars/postgresql.jar \
    https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

WORKDIR /opt/spark
