from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="telemetry_spark_analytics",
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_spark_job = BashOperator(
        task_id="run_telemetry_analytics",
        bash_command="""
        docker exec telemetry_spark \
        /opt/spark/bin/spark-submit \
        --master local[*] \
        --jars /opt/spark/jars/postgresql.jar \
        /opt/spark/jobs/telemetry_analytics_job.py
        """
    )
