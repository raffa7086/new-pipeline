from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from bronze_capture_equities import safe_capture_equities


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_capture_equities",
    description="Captura cotações diárias da Alpha Vantage e salva na camada Bronze",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "equities", "alphavantage"],
) as dag:

    t_capture = PythonOperator(
        task_id="capture_and_save_equities",
        python_callable=safe_capture_equities,
    )
