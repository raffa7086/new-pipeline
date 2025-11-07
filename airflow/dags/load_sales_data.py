from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

# Função de extrção de dados
def extract_data():
    df = pd.read_csv("/opt/airflow/dbt/vendas.csv") # Lendo o arquivo CSV e criando um DF (como uma planilha em memória).
    print("Dados extraídos com sucesso:")
    print(df.head())
    df.to_csv("/opt/airflow/dbt/vendas_temp.csv", index=False) # Salvando o DF temporariamente para a próxima etapa.

# Função de transformação de dados
def transform_data():
    df = pd.read_csv("/opt/airflow/dbt/vendas_temp.csv")
    df = df.dropna(subset=["valor_total", "quantidade"]) # Removendo as linhas com dados faltando
    df["valor_total"] = df["valor_total"].astype(float)
    df["quantidade"] = df["quantidade"].astype(int)
    df["preco_unitario"] = df["valor_total"] / df["quantidade"]
    df.to_csv("/opt/airflow/dbt/vendas_tratado.csv", index=False)
    print("Transformação concluída:")
    print(df.head())

# Função de carga de dados
def load_data():
    # Conectamos ao banco Postgres que está rodando dentro do Docker.
    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="postgres",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS vendas_tratadas (
            id_venda INT,
            produto TEXT,
            quantidade INT,
            valor_total FLOAT,
            data_venda DATE,
            preco_unitario FLOAT
        );
    """)
    conn.commit()

    df = pd.read_csv("/opt/airflow/dbt/vendas_tratado.csv")
    for _, row in df.iterrows():
        cur.execute(
            "INSERT INTO vendas_tratadas VALUES (%s, %s, %s, %s, %s, %s)",
            (row.id_venda, row.produto, row.quantidade, row.valor_total, row.data_venda, row.preco_unitario)
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Dados carregados no Postgres com sucesso!")

# Definição do DAG
with DAG(
    # nome do pipeline.
    dag_id="load_sales_data",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,  # manual
    catchup=False,
    tags=["pipeline", "vendas"],
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    extract >> transform >> load
