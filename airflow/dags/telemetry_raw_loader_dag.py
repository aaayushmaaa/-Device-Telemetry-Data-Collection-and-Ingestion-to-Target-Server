from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

    # Fetch only unprocessed files
    cursor.execute("""
        SELECT object_name
        FROM telemetry_files
        WHERE processed = FALSE
    """)
    files_to_process = cursor.fetchall()

    for (object_name,) in files_to_process:
        try:
            response = MINIO_CLIENT.get_object(BUCKET_NAME, object_name)
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

            # Mark file as processed only after successful insert
            cursor.execute(
                """
                UPDATE telemetry_files
                SET processed = TRUE, ingested_at = NOW()
                WHERE object_name = %s
                """,
                (object_name,)
            )

            print(f"✅ Processed {object_name} successfully")

        except Exception as e:
            print(f"❌ Failed to process {object_name}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="telemetry_raw_loader",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Triggered manually or via API
    catchup=False,
    tags=["telemetry", "raw"],
) as dag:

    load_raw = PythonOperator(
        task_id="load_raw_events",
        python_callable=load_raw_events
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="telemetry_transform_dag",
    )

load_raw >> trigger_transform
