import io
import os
from datetime import datetime, timedelta

import boto3
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

# Variáveis de ambiente para credenciais AWS
account_key = os.getenv('AWS_ACCESS_KEY')
secret_key = os.getenv('AWS_SECRET_KEY')


# Função para baixar o arquivo do S3
def download_dataset():
    s3 = boto3.client('s3', aws_access_key_id=account_key, aws_secret_access_key=secret_key)
    bucket_name = 'investimentos-govbr'
    object_key = 'raw/investimentos_2024.csv'
    local_file_path = '/tmp/investimentos_2024.csv'
    try:
        # Faz o download do arquivo do S3
        s3.download_file(bucket_name, object_key, local_file_path)
        print(f"Arquivo {object_key} baixado para {local_file_path}")
    except s3.exceptions.NoSuchKey:
        print(f"Arquivo {object_key} não encontrado no bucket {bucket_name}. Verifique o caminho.")


# Função para fazer o upload do arquivo processado para o S3
def upload_to_s3(local_file_path, s3_bucket, s3_key):
    s3 = boto3.client('s3', aws_access_key_id=account_key, aws_secret_access_key=secret_key)
    try:
        # Faz o upload do arquivo
        s3.upload_file(local_file_path, s3_bucket, s3_key)
        print(f"Arquivo {local_file_path} enviado para {s3_bucket}/{s3_key}")
    except Exception as e:
        print(f"Erro ao enviar o arquivo {local_file_path} para o S3: {str(e)}")


# Função para transformar os dados
def transform_data_s3():
    s3 = boto3.client('s3', aws_access_key_id=account_key, aws_secret_access_key=secret_key)
    bucket_name = 'investimentos-govbr'
    source_key = 'raw/investimentos_2024.csv'
    dest_key = 'processed/investimentos_2024_mod.csv'
    # Lê o arquivo diretamente do S3
    response = s3.get_object(Bucket=bucket_name, Key=source_key)
    csv_content = response['Body'].read().decode('latin1')  # Use o encoding correto
    # Carregar o CSV no Pandas
    df = pd.read_csv(io.StringIO(csv_content), delimiter=";")
    # Realiza a transformação dos dados
    df.columns = df.columns.str.strip()  # Remove espaços nos nomes das colunas

    # Converter valores para Inteiro com tratamento de erros
    col_for_int = [
        'ano', 
        'esfera_orcamentaria', 
        'orgao_maximo', 
        'uo', 
        'grupo_despesa', 
        'aplicacao', 
        'resultado', 
        'funcao', 
        'subfuncao', 
        'programa'
        ]
    
    for col in col_for_int:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        
    # Limpar e converter a coluna 'movimento_liquido_reais'
    df['movimento_liquido_reais'] = (
        df['movimento_liquido_reais']
        .str.replace('.', '', regex=False)  # Remove os separadores de milhar
        .str.replace(',', '.', regex=False)  # Substitui a vírgula decimal por ponto
        .str.replace(r'\((.*?)\)', r'-\1', regex=True)  # Converte (valor) para -valor
        .astype(float)  # Converte para float
    )

    # Salva o arquivo transformado localmente
    local_file_path = '/tmp/investimentos_2024_mod.csv'
    df.to_csv(local_file_path, index=False, sep=',')
    # Faz upload do arquivo transformado para a pasta processed/ no S3
    upload_to_s3(local_file_path, bucket_name, dest_key)


# Função para carregar os dados no PostgreSQL
def load_data_to_postgres():
    
    # Conexão ao S3
    s3 = boto3.client('s3', aws_access_key_id=account_key, aws_secret_access_key=secret_key)
    bucket_name = 'investimentos-govbr'
    file_key = 'processed/investimentos_2024_mod.csv'

    # Lê o arquivo transformado diretamente do S3
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    # Detecta os tipos de dados e mapeia para tipos SQL
    dtype_mapping = {
        'int64': 'INT',
        'float64': 'NUMERIC',
        'object': 'VARCHAR',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP',
    }


    column_types = {}
    for column in df.columns:
        dtype = str(df[column].dtype)
        sql_type = dtype_mapping.get(dtype, 'VARCHAR')  # Padrão para VARCHAR
        column_types[column] = sql_type

    # Cria o comando SQL de criação de tabela dinamicamente
    table_name = "investimentos"
    columns_sql = ",\n".join([f"{col} {col_type}" for col, col_type in column_types.items()])
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns_sql}
    )
    """

    # Conexão ao PostgreSQL
    conn = psycopg2.connect(
        host='postgres',
        database='airflow',
        user='airflow',
        password='airflow'
    )
    cursor = conn.cursor()

    try:
        # Cria a tabela
        cursor.execute(create_table_sql)
        conn.commit()

        # Insere os dados
        for _, row in df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            insert_sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({placeholders})"
            cursor.execute(insert_sql, tuple(row))

        conn.commit()
        print("Dados carregados com sucesso!")
    except Exception as e:
        conn.rollback()
        print(f"Erro ao carregar os dados: {e}")
    finally:
        cursor.close()
        conn.close()

    
# Configurações básicas da DAG
default_args = {
    'owner': 'Sisnando Junior',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=2),
    "max_active_runs_per_dag": 1,
}

with DAG(
    dag_id='etl_investimentos_publicos',
    default_args=default_args,
    description='Pipeline ETL para processar dados de investimentos públicos no S3 e carregar no PostgreSQL',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 12, 1),
    tags=['ETL para S3 AWS'],
    catchup=False
) as dag:

    # Define as tarefas da DAG
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_dataset
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_s3
    )

    load_to_db = PythonOperator(
        task_id='load_to_db',
        python_callable=load_data_to_postgres
    )

    # Define a ordem das tarefas
    download_data >> transform_data >> load_to_db