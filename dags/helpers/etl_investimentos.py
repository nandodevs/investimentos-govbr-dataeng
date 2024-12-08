import os
from datetime import datetime
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# URL do dataset
DATASET_URL = "https://www.tesourotransparente.gov.br/ckan/dataset/e048826b-b6b0-4d92-9204-fd218b1f25b3/resource/5748b19a-c652-4327-a5a0-e09109e8f9e9/download/Investimentos---TT---2024.csv"
LOCAL_FILE_PATH = "/opt/airflow/data/investimentos_2024.csv"
S3_BUCKET_NAME = "investimentos-govbr"
S3_KEY = "raw/investimentos_2024.csv"

# Função para baixar o dataset
def download_dataset():
    response = requests.get(DATASET_URL)
    response.raise_for_status()  # Garante que erros sejam levantados
    os.makedirs(os.path.dirname(LOCAL_FILE_PATH), exist_ok=True)
    with open(LOCAL_FILE_PATH, "wb") as file:
        file.write(response.content)
    print(f"Dataset baixado e salvo em {LOCAL_FILE_PATH}")

# Função para salvar no S3
def upload_to_s3():
    s3 = S3Hook(aws_conn_id="aws_default")  # Certifique-se de configurar esta conexão no Airflow
    s3.load_file(
        filename=LOCAL_FILE_PATH,
        bucket_name=S3_BUCKET_NAME,
        key=S3_KEY,
        replace=True
    )
    print(f"Arquivo carregado no S3: s3://{S3_BUCKET_NAME}/{S3_KEY}")

# Configuração da DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 300,  # 5 minutos
}

with DAG(
    dag_id="etl_investimentos_2024",
    default_args=default_args,
    description="Pipeline ETL - Investimentos Públicos 2024",
    #schedule_interval="@daily",
    start_date=datetime(2023, 12, 1),
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id="download_dataset",
        python_callable=download_dataset,
    )

    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    download_task >> upload_task
