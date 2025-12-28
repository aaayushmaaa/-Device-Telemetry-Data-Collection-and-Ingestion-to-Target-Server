#  Telemetry Data Collection & Processing Pipeline

An end-to-end telemetry data pipeline that collects data from a mobile application, validates and tokenizes sensitive fields, stores raw data in a data lake, processes it using distributed computing, and loads analytics-ready data into a relational database.

---

##  Project Overview

This project demonstrates a **production-style telemetry ingestion and processing pipeline** using modern data engineering tools.

Telemetry data is:
- Collected from a **Flutter mobile application**
- Ingested via a **FastAPI service**
- Stored in **MinIO (data lake)**
- Orchestrated using **Apache Airflow**
- Processed and transformed using **Apache Spark**
- Loaded into **PostgreSQL** for analytics and reporting

The system is designed to **process only new (unprocessed) telemetry files**, ensuring idempotency and scalability.

---

##  System Architecture

```
Flutter App
    │
    ▼
FastAPI (Telemetry Ingestion API)
├─ Schema validation (Pydantic)
├─ Tokenization of sensitive fields
└─ Metadata tracking
    │
    ▼
MinIO (S3-compatible Data Lake)
└─ Raw telemetry JSON files
    │
    ▼
Apache Airflow (Orchestration)
    │
    ▼
Apache Spark (Processing Engine)
├─ Transform telemetry data
└─ Enrich events
    │
    ▼
PostgreSQL (Analytics Storage)
```
---

##  Tech Stack

| Layer | Technology |
|-----|-----------|
| Mobile Client | Flutter |
| API Layer | FastAPI |
| Validation | Pydantic |
| Data Lake | MinIO |
| Orchestration | Apache Airflow |
| Processing | Apache Spark (PySpark) |
| Database | PostgreSQL |
| Containerization | Docker & Docker Compose |

---

### Project Structure

```
├─ flutter_app/ # Flutter mobile client
│ ├─ lib/ # Flutter Dart source files
│ └─ pubspec.yaml # Flutter dependencies
│
├─ fastapi_ingestion/ # FastAPI API service
│ ├─ main.py # FastAPI application entry point
│ ├─ requirements.txt # Python dependencies
│ └─ Dockerfile # Container definition
│
├─ dags/ # Apache Airflow DAGs
│ ├─ telemetry_raw_loader_dag.py # Loads raw telemetry from MinIO
│ └─ telemetry_transform_dag.py # Processes and enriches telemetry
│
├─ spark/ # Apache Spark jobs
│ └─ telemetry_processing.py # Spark transformations and enrichment
│
├─ docker-compose.yml # Docker Compose configuration for all services
├─ .env # Environment variables for services
├─ README.md # Project documentation
└─ scripts/ # Optional utility scripts
```
---

##  Data Flow (Step-by-Step)

1. **Telemetry Collection**
    - Flutter app collects device, session, battery, app, and network telemetry.
    - Data is sent to FastAPI using a secure API key.

2. **Ingestion & Validation**
    - FastAPI validates incoming payloads using **Pydantic**.
    - Sensitive fields (e.g., network data) are tokenized using SHA-256.
    - Raw structured telemetry is stored in PostgreSQL for quick access.

3. **Raw Storage (Data Lake)**
    - Tokenized telemetry data is stored as JSON files in **MinIO**.
    - Each file is registered in PostgreSQL with a `processed = false` flag.

4. **Pipeline Orchestration**
    - FastAPI triggers the **Airflow DAG** (or DAG can run on schedule).
    - Airflow manages task execution and dependencies.

5. **Data Processing**
    - Spark reads only **unprocessed files** from MinIO.
    - Telemetry data is transformed, flattened, and enriched.
    - Processed data is inserted into analytics tables in PostgreSQL.

6. **Post-Processing**
    - Successfully processed files are marked as `processed = true`.
    - Prevents duplicate processing on future runs.

---

##  Database Tables

### 1. `telemetry_staging`
Stores raw structured telemetry received from FastAPI.

| Column  | Description                  |
|---------|------------------------------|
| id      | Unique event ID              |
| source  | Data source (mobile-sdk)     |
| payload | Raw JSON telemetry           |

---

### 2. `telemetry_files`
Tracks MinIO objects and processing status.

| Column       | Description                     |
|--------------|---------------------------------|
| object_name  | MinIO file path                 |
| processed    | Boolean processing flag         |
| created_at   | File registration timestamp     |

---

### 3. `raw_events`
Stores telemetry loaded from MinIO by the raw loader DAG.

| Column      | Description                  |
|-------------|------------------------------|
| event_id    | Unique event ID              |
| source      | Data source (mobile-sdk)     |
| device_id   | Device identifier            |
| event_time  | Event timestamp              |
| payload     | Raw JSON telemetry           |

---

### 4. `enriched_events`
Stores cleaned, transformed, analytics-ready telemetry data (processed by Spark).

| Column      | Description                  |
|-------------|------------------------------|
| event_id    | Unique event ID              |
| device_id   | Device identifier            |
| event_time  | Event timestamp              |
| metrics     | Flattened / enriched telemetry fields |


##  Apache Spark Role

Apache Spark is responsible for:
- Reading raw telemetry JSON files from MinIO
- Transforming nested telemetry structures
- Enriching data with derived fields
- Writing clean data into PostgreSQL
- Ensuring scalable and distributed processing

Spark is triggered and monitored by **Apache Airflow**.

---

##  Key Features

- Schema validation with Pydantic
- Tokenization of sensitive data
- Idempotent processing (only new files)
- Distributed processing using Spark
- Workflow orchestration with Airflow
- S3-compatible data lake using MinIO
- Fully containerized using Docker

---

##  Running the Project

### Prerequisites
- Docker & Docker Compose
- Git

### Start the services
```bash
git clone https://github.com/aaayushmaaa/-Device-Telemetry-Data-Collection-and-Ingestion-to-Target-Server
docker-compose up --build
```
### Accessing Services

- **FastAPI docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **Airflow UI**: [http://localhost:8080](http://localhost:8080)
    - Username: `admin`
    - Password: `admin`
- **PostgreSQL**: Connect via port `5432` (use your DB client)
- **MinIO**: [http://localhost:9000](http://localhost:9000)
    - Username: `admin`
    - Password: `admin123`

---

### Future Improvements / Next Steps

- Add authentication/authorization for API endpoints
- Add monitoring and alerting for failed DAG runs
- Enable streaming ingestion from mobile app
- Add data quality checks in Spark processing
- Deploy to cloud (AWS/GCP/Azure)

---

## Conclusion

This project demonstrates a complete **end-to-end telemetry data pipeline** that collects, validates, tokenizes, stores, processes, and enriches telemetry data from a mobile application.  
