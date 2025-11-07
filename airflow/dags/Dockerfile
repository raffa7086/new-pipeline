# Imagem base oficial do Airflow (isso evita instalar tudo manualmente)
FROM apache/airflow:2.7.3-python3.10

# Define o diretório padrão dentro do container
WORKDIR /opt/airflow

# Copia o arquivo de dependências para dentro da imagem
COPY requirements.txt .

# Instala dependências extras (dbt, pandas, etc.)
RUN pip install --no-cache-dir -r requirements.txt

# Copia as DAGs para dentro do container
COPY dags/ ./dags
