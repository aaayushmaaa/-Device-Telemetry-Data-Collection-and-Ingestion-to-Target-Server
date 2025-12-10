from airflow import DAG
from datetime import datetime

with DAG(
    "placeholder",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None
) as dag:
    pass
