from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
import psycopg2
import os

# URL do dataset
account_key = os.getenv('AWS_ACCESS_KEY')
secret_key = os.getenv('AWS_SECRET_KEY')
PATH_TPM = '/opt/airflow/data/processed/investimentos_2024.csv'

# Função para baixar o arquivo do S3
def download_data_from_s3(**kwargs):
    session = boto3.Session(
        aws_access_key_id=account_key,
        aws_secret_access_key=secret_key
    )
    s3 = session.client('s3')  # Inicializa o cliente S3
    bucket_name = 'investimentos-govbr'
    object_key = 'raw/investimentos_2024.csv'
    
    os.makedirs('/opt/airflow/data/processed', exist_ok=True)  # Garante que a pasta existe
    download_path = PATH_TPM

    # Faz o download do arquivo
    s3.download_file(bucket_name, object_key, download_path)
    print(f"Arquivo baixado com sucesso em: {download_path}")



def transform_data(**kwargs):
    file_path = PATH_TPM
    df = pd.read_csv(file_path, encoding='latin1', delimiter=';')
    # Realiza limpeza básica
    df.columns = df.columns.str.strip()  # Remove espaços nos nomes das colunas
    df.dropna(inplace=True)  # Remove linhas com valores nulos
    # Salva o arquivo transformado
    transformed_path = PATH_TPM
    df.to_csv(transformed_path, index=False)
    print(f"Dados transformados e salvos em {transformed_path}")

# Função para criar a tabela dinamicamente no PostgreSQL
def create_table_if_not_exists(df, **kwargs):
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    # Cria a tabela de acordo com as colunas do DataFrame
    columns = ', '.join([f'"{col}" TEXT' for col in df.columns])  # Criar as colunas dinamicamente
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS investimentos (
            {columns}
        );
    """
    cursor.execute(create_table_query)
    conn.commit()
    print("Tabela investimentos_2024 criada com sucesso!")
    cursor.close()
    conn.close()

# Função para carregar os dados no PostgreSQL
def load_data_to_postgres(**kwargs):
    transformed_path = PATH_TPM
    df = pd.read_csv(transformed_path)
    # Conectar ao PostgreSQL
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()
    # Carregar os dados no banco
    for _, row in df.iterrows():
        query = f"""
            INSERT INTO investimentos ({', '.join(df.columns)})
            VALUES ({', '.join(['%s'] * len(row))})
        """
        cursor.execute(query, tuple(row))
    conn.commit()
    cursor.close()
    conn.close()
    print("Dados carregados no banco de dados!")

# Configurações básicas da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_investimentos_publicos',
    default_args=default_args,
    description='Pipeline ETL para processar dados de investimentos públicos',
    schedule_interval='@daily',
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    # Define as tarefas da DAG
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_data_from_s3
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table_if_not_exists,
        op_kwargs={'df': pd.read_csv(PATH_TPM, delimiter=";")}  # Passa o DataFrame para a criação da tabela
    )

    load_to_db = PythonOperator(
        task_id='load_to_db',
        python_callable=load_data_to_postgres
    )
    
# Define a ordem das tarefas
download_data >> transform_data >> create_table >> load_to_db
