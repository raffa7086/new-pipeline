from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG que executa dbt dentro do container do Airflow
with DAG(
    dag_id="dbt_example_dag",
    start_date=datetime(2025, 11, 1),
    schedule=None,      # roda manualmente; nada de backfill
    catchup=False,
    tags=["dbt", "example"],
) as dag:

    # 1) baixa dependências do dbt
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps --profiles-dir ."
    )

    # 2) carrega o CSV de seeds (dbt/seeds/sample_data.csv) para o Postgres
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /opt/airflow/dbt && dbt seed --profiles-dir ."
    )

    # 3) roda os modelos SQL (dbt/models/*.sql)
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir ."
    )

    dbt_deps >> dbt_seed >> dbt_run
