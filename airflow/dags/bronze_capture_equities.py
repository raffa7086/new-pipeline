from __future__ import annotations
import os
import requests
import pandas as pd
from datetime import datetime, timezone


# ======================================================
# 🔹 Função principal: captura os dados da Alpha Vantage
# ======================================================
def get_commodities_df() -> pd.DataFrame:
    """
    Captura as últimas cotações diárias de ações (AAPL, MSFT, GOOG)
    na API Alpha Vantage e retorna um DataFrame com as colunas:
    ativo, preco, moeda e horario_coleta.
    """
    API_KEY = os.getenv("CHAVE_API")
    symbols = ["AAPL", "MSFT", "GOOG"]
    rows = []

    print(f"CHAVE_API: {API_KEY[:6] + '...' if API_KEY else 'Não encontrada'}")

    for sym in symbols:
        print(f"\n📊 Buscando {sym}...")
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={sym}&apikey={API_KEY}"
        r = requests.get(url, timeout=30)

        # Validação básica
        if r.status_code != 200:
            print(f"Erro HTTP {r.status_code} para {sym}")
            continue

        data = r.json()
        ts = data.get("Time Series (Daily)", {})

        if not ts:
            print(f"Sem dados para {sym}.")
            continue

        ultima_data = sorted(ts.keys())[-1]
        ultimo = ts[ultima_data]
        preco_fechamento = float(ultimo["4. close"])

        rows.append({
            "ativo": sym,
            "preco": preco_fechamento,
            "moeda": "USD",
            "horario_coleta": datetime.now(timezone.utc).isoformat()
        })

    # Cria DataFrame final
    df = pd.DataFrame(rows)
    print("\nPrévia do DataFrame coletado:")
    print(df)
    return df


# ======================================================
# 🔹 Função auxiliar: salva o DataFrame na camada Bronze
# ======================================================
def save_to_bronze(df: pd.DataFrame) -> str:
    """
    Salva o DataFrame como JSON na pasta Bronze.
    Retorna o caminho do arquivo salvo.
    """
    BRONZE_DIR = os.getenv("CAMINHO_BRONZE", "/opt/airflow/dbt/bronze")
    os.makedirs(BRONZE_DIR, exist_ok=True)

    file_name = os.path.join(
        BRONZE_DIR,
        f"alphavantage_equities_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    )

    df.to_json(file_name, orient="records", lines=True, force_ascii=False)
    print(f"\n✅ JSON salvo em: {file_name}")
    return file_name


# ======================================================
# 🔹 Função segura (usada pela DAG)
# ======================================================
def safe_capture_equities():
    """
    Captura e salva cotações via Alpha Vantage, 
    sem quebrar se houver erro ou limite de API.
    """
    try:
        df = get_commodities_df()

        if df.empty:
            print("⚠️ Nenhum dado retornado pela API (possível limite atingido).")
            return None

        path = save_to_bronze(df)
        print(f"✅ Dados salvos em {path}")
        return path

    except Exception as e:
        print(f"❌ Erro ao capturar ou salvar dados: {e}")
        return None

