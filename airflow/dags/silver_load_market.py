from sqlalchemy import create_engine, text
import pandas as pd
import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# =========================
# Configurações gerais
# =========================
BRONZE_DIR = os.getenv("BRONZE_DIR", "/opt/airflow/dbt/bronze")
ENGINE = create_engine(
    "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow",
    pool_pre_ping=True,
    echo=False
)

# =========================
# Função 1 – Treasury Yield
# =========================
def load_treasury_yield():
    """Carrega arquivos JSON de Treasury Yield da Bronze e escreve na camada Silver"""
    files = [f for f in os.listdir(BRONZE_DIR) if f.startswith("treasury_")]
    if not files:
        print("Nenhum arquivo treasury_* encontrado.")
        return

    # garante schema "silver" no Postgres
    with ENGINE.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))

    for file in files:
        path = os.path.join(BRONZE_DIR, file)
        with open(path, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Arquivo {file} inválido. Pulando.")
                continue

        # trata arquivos vazios ou sem chave data
        if not data or "data" not in data:
            print(f"Arquivo {file} vazio ou sem chave 'data'. Criando tabela vazia.")
            df = pd.DataFrame(columns=["date", "value"])
        else:
            df = pd.DataFrame(data["data"])

        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        if "value" in df.columns:
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

        with ENGINE.connect() as conn:
            df.to_sql(
                "treasury_yield",
                con=conn,
                schema="silver",
                if_exists="replace",
                index=False
            )
        print(f"[silver] tabela treasury_yield criada ({len(df)} linhas)")


# =========================
# Função 2 – Indicadores Técnicos
# =========================
def load_indicators():
    """Carrega arquivos JSON de indicadores da Bronze e escreve na camada Silver"""
    files = [f for f in os.listdir(BRONZE_DIR) if f.startswith("indicator_")]
    if not files:
        print("Nenhum arquivo indicator_* encontrado.")
        return

    with ENGINE.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver;"))

    for file in files:
        path = os.path.join(BRONZE_DIR, file)
        with open(path, "r") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"Arquivo {file} inválido. Pulando.")
                continue

        tech_key = next((k for k in data.keys() if k.startswith("Technical Analysis")), None)
        if not data or not tech_key:
            print(f"Arquivo {file} vazio ou sem chave técnica. Criando tabela vazia.")
            df = pd.DataFrame(columns=["date", "value"])
        else:
            df = pd.DataFrame.from_dict(data[tech_key], orient="index").reset_index()
            df.rename(columns={"index": "date", list(df.columns)[1]: "value"}, inplace=True)

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        with ENGINE.connect() as conn:
            df.to_sql(
                "indicators",
                con=conn,
                schema="silver",
                if_exists="replace",
                index=False
            )
        print(f"[silver] tabela indicators criada ({len(df)} linhas)")


# =========================
# DAG
# =========================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="silver_load_market",
    description="Carrega dados da camada Bronze para Silver (Postgres)",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False,
    tags=["silver", "load"],
    default_args=default_args,
) as dag:

    t_treasury = PythonOperator(
        task_id="load_treasury_yield",
        python_callable=load_treasury_yield,
    )

    t_indicators = PythonOperator(
        task_id="load_indicators",
        python_callable=load_indicators,
    )

    t_treasury >> t_indicators
