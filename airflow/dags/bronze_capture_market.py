from __future__ import annotations

import os
import json
import time
from datetime import datetime, timedelta
from typing import List, Dict

import yfinance as yf
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# -----------------------------
# Configuração e parâmetros
# -----------------------------
BRONZE_DIR = os.getenv("CAMINHO_BRONZE", "/opt/airflow/dbt/bronze")

TICKERS: List[str] = [
    "AAPL",
    "MSFT",
    "PETR4.SA",
    "VALE3.SA"
]

# -----------------------------
# Funções utilitárias
# -----------------------------
def _ensure_bronze_dir():
    os.makedirs(BRONZE_DIR, exist_ok=True)

def _save_json(content: Dict, filename: str):
    """Salva JSON na Bronze com timestamp para versionamento."""
    _ensure_bronze_dir()
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(BRONZE_DIR, f"{filename}_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(content, f, ensure_ascii=False, indent=2)
    print(f"[bronze] salvo: {path}")

# -----------------------------
# Tarefas (Python callables)
# -----------------------------
def capture_daily_prices():
    """Captura preços históricos diários das ações via Yahoo Finance."""
    for ticker in TICKERS:
        print(f"Capturando histórico de {ticker}...")
        try:
            df = yf.download(ticker, period="1mo", interval="1d", progress=False)
            if df.empty:
                print(f"Nenhum dado retornado para {ticker}")
                continue

            df.reset_index(inplace=True)
            df["Ticker"] = ticker
            data_dict = df.to_dict(orient="records")
            _save_json(data_dict, f"daily_prices_{ticker}")

            print(f"Dados de {ticker} salvos com sucesso.")
        except Exception as e:
            print(f"Erro ao capturar {ticker}: {e}")
        time.sleep(3)

def capture_treasury_yield():
    """Simula captura de Treasury Yield com dados reais do Yahoo Finance (US10Y)."""
    try:
        print("Capturando Treasury Yield (10 anos)...")
        df = yf.download("^TNX", period="1mo", interval="1d", progress=False)
        df.reset_index(inplace=True)
        df.rename(columns={"Close": "Yield"}, inplace=True)
        data_dict = df[["Date", "Yield"]].to_dict(orient="records")
        _save_json(data_dict, "treasury_10y_yield")
        print("Treasury Yield salvo com sucesso.")
    except Exception as e:
        print(f"Erro ao capturar Treasury Yield: {e}")

def capture_one_indicator():
    """Calcula um indicador técnico simples (RSI) localmente usando pandas."""
    ticker = TICKERS[0]
    print(f"Calculando RSI para {ticker}...")
    try:
        df = yf.download(ticker, period="3mo", interval="1d", progress=False)
        delta = df["Close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        df["RSI"] = 100 - (100 / (1 + rs))
        df.reset_index(inplace=True)
        data_dict = df[["Date", "Close", "RSI"]].dropna().to_dict(orient="records")
        _save_json(data_dict, f"indicator_RSI_{ticker}")
        print(f"RSI calculado e salvo para {ticker}")
    except Exception as e:
        print(f"Erro ao calcular RSI: {e}")

# -----------------------------
# DAG
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_capture_market",
    description="Captura dados brutos de ações, treasury e indicadores via Yahoo Finance e salva na camada Bronze",
    default_args=default_args,
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "market", "yfinance"],
) as dag:

    t_daily = PythonOperator(
        task_id="capture_daily_prices",
        python_callable=capture_daily_prices,
    )

    t_treasury = PythonOperator(
        task_id="capture_treasury_yield",
        python_callable=capture_treasury_yield,
    )

    t_indicator = PythonOperator(
        task_id="capture_one_indicator",
        python_callable=capture_one_indicator,
    )

    [t_daily, t_treasury, t_indicator]
