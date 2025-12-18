from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import uuid
import psycopg2
from minio import Minio
import io

# -----------------------------
# Config
# -----------------------------
POSTGRES_CONN = {
    "host": "postgres",
    "dbname": "telemetrydb",
    "user": "telemetry",
    "password": "telemetry",
    "port": 5432,
}

MINIO_CLIENT = Minio(
    endpoint="minio:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False,
)

BUCKET_NAME = "telemetry"

# -----------------------------
# Task logic
# -----------------------------
def load_raw_events():
    conn = psycopg2.connect(**POSTGRES_CONN)
    cursor = conn.cursor()

    objects = MINIO_CLIENT.list_objects(
        BUCKET_NAME,
        prefix="telemetry/",
        recursive=True
    )

    for obj in objects:
        response = MINIO_CLIENT.get_object(BUCKET_NAME, obj.object_name)
        data = json.loads(response.read().decode())

        event_id = uuid.uuid4()
        device_id = data.get("device", {}).get("id", "unknown")
        event_time = data.get("session", {}).get("timestamp")

        cursor.execute(
            """
            INSERT INTO raw_events (event_id, source, device_id, event_time, payload)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                str(event_id),
                "mobile-sdk",
                device_id,
                event_time,
                json.dumps(data)
            )
        )

    conn.commit()
    cursor.close()
    conn.close()

# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="telemetry_raw_loader",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["telemetry", "raw"],
) as dag:

    load_raw = PythonOperator(
        task_id="load_raw_events",
        python_callable=load_raw_events
    )
