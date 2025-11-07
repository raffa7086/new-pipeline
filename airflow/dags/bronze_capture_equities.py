from __future__ import annotations
import os
import requests
import pandas as pd
from datetime import datetime, timezone

# --- Etapa 1: Configurações básicas ---
# Variáveis de ambiente vindas do .env
API_KEY = os.getenv("CHAVE_API")
BRONZE_DIR = os.getenv("CAMINHO_BRONZE", "/opt/airflow/dbt/bronze")

print(f"CHAVE_API: {API_KEY[:6]+'...' if API_KEY else 'Não encontrada'}")   # mostra só início, pra não expor
print(f"Caminho Bronze: {BRONZE_DIR}")

# --- Etapa 2: Teste da API Alpha Vantage ---
url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=AAPL&apikey={API_KEY}"

print(f"Chamando: {url}")

r = requests.get(url, timeout=30) # requests.get() faz a chamada real à API Alpha Vantage
print(f"Status HTTP: {r.status_code}") # r.status_code mostra se a conexão deu certo (200 = OK)

try:
    data = r.json() # r.json() transforma o retorno bruto em dicionário Python.
    print(f"Chaves no JSON: {list(data.keys())[:5]}")
except Exception as e:
    print(f"Erro ao converter para JSON: {e}")

# --- Etapa 3: Examinar a estrutura interna ---
ts = data.get("Time Series (Daily)", {}) # data.get("Time Series (Daily)", {}) → acessa a parte do JSON onde estão os preços diários.

if not ts:
    print("Nenhum dado encontrado em 'Time Series (Daily)'.")
else:
    # Pega as 2 primeiras datas (mais recentes)
    keys = list(ts.keys())[:2] #list(ts.keys())[:2] → mostra só duas datas pra não poluir o terminal.
    print(f"Amostras de datas: {keys}")

    for date in keys:
        print(f"{date} -> {ts[date]}") # ts[date] → mostra o conteúdo da data (ex: open, high, low, close, volume).


# --- Etapa 4: Extrair o último valor ---
ultima_data = sorted(ts.keys())[-1]
ultimo = ts[ultima_data]

preco_fechamento = float(ultimo["4. close"])
print(f"\n✅ Último registro:")
print(f"Data: {ultima_data}")
print(f"Preço de fechamento: {preco_fechamento}")


# --- Etapa 5: Montar DataFrame com múltiplos ativos ---
symbols = ["AAPL", "MSFT", "GOOG"]
rows = []

for sym in symbols:
    print(f"\n📊 Buscando {sym}...")
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={sym}&apikey={API_KEY}"
    r = requests.get(url, timeout=30)
    data = r.json()
    ts = data.get("Time Series (Daily)", {})
    if not ts:
        print(f"⚠️ Sem dados para {sym}.")
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

# Cria DataFrame
df = pd.DataFrame(rows)
print("\n📈 Prévia do DataFrame:")
print(df)

# --- Etapa 6: Salvar na camada Bronze ---
file_name = os.path.join(
    BRONZE_DIR,
    f"alphavantage_equities_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
)

df.to_json(file_name, orient="records", lines=True, force_ascii=False)
print(f"\n✅ JSON salvo em: {file_name}")