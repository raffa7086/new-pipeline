# Usa imagem oficial do Apache Airflow
FROM apache/airflow:2.7.3-python3.10

# Define diretório de trabalho
WORKDIR /opt/airflow

# Copia arquivo de dependências
COPY requirements.txt .

# Instala pacotes extras com segurança
RUN pip install --no-cache-dir -r requirements.txt || echo "Falha em algumas libs, mas continuando..."

# Copia DAGs para dentro do container
COPY dags/ ./dags/
