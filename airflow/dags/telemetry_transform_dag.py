from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

POSTGRES_CONN = {
    "host": "postgres",
    "database": "telemetrydb",
    "user": "telemetry",
    "password": "telemetry"
}

def transform_raw_events(**kwargs):
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Fetch raw events
    cur.execute("SELECT event_id, source, device_id, event_time, payload FROM raw_events")
    rows = cur.fetchall()

    if not rows:
        print("No raw events to process.")
        cur.close()
        conn.close()
        return

    # Transform / enrich
    enriched_rows = []
    for row in rows:
        payload = row["payload"]

        app_version = payload.get("app", {}).get("version")
        battery_level = payload.get("battery", {}).get("level")

        enriched_rows.append((
            row["event_id"],
            row["source"],
            row["device_id"],
            row["event_time"],
            app_version,
            battery_level,
            datetime.utcnow()
        ))

    # Insert into enriched_events
    insert_query = """
        INSERT INTO enriched_events
        (event_id, source, device_id, event_time, app_version, battery_level, inserted_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """

    cur.executemany(insert_query, enriched_rows)
    conn.commit()
    print(f"Inserted {len(enriched_rows)} enriched events.")

    cur.close()
    conn.close()


with DAG(
    dag_id="telemetry_transform_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@hourly",
    catchup=False
) as dag:

    transform_task = PythonOperator(
        task_id="transform_raw_events",
        python_callable=transform_raw_events,
        provide_context=True
    )
