from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
import io
import os
import psycopg2

# Variáveis de ambiente para credenciais AWS
account_key = os.getenv('AWS_ACCESS_KEY')
secret_key = os.getenv('AWS_SECRET_KEY')

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
    description='Pipeline ETL para processar dados de investimentos públicos no S3 e carregar no PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 12, 1),
    catchup=False,
) as dag:

    # Função para baixar o arquivo do S3
    def download_dataset(**kwargs):
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
    def transform_data_s3(**kwargs):
        s3 = boto3.client('s3', aws_access_key_id=account_key, aws_secret_access_key=secret_key)
        bucket_name = 'investimentos-govbr'
        source_key = 'raw/investimentos_2024.csv'
        dest_key = 'processed/investimentos_2024_mod.csv'

        # # Lê o arquivo diretamente do S3
        # obj = s3.get_object(Bucket=bucket_name, Key=source_key)
        # df = pd.read_csv(io.BytesIO(obj['Body'].read().decode('latin-1')))

        # Baixando o arquivo CSV do S3
        response = s3.get_object(Bucket=bucket_name, Key=source_key)
        csv_content = response['Body'].read().decode('latin1')  # Use o encoding correto

        # Carregar o CSV no Pandas
        df = pd.read_csv(io.StringIO(csv_content), delimiter=",")

        # Realiza a transformação dos dados
        df.columns = df.columns.str.strip()  # Remove espaços nos nomes das colunas
        df.dropna(inplace=True)  # Remove linhas com valores nulos

        # Passo 1: Limpar e converter a coluna 'movimento_liquido_reais'
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
    def load_data_to_postgres(**kwargs):
        s3 = boto3.client('s3', aws_access_key_id=account_key, aws_secret_access_key=secret_key)
        bucket_name = 'investimentos-govbr'
        file_key = 'processed/investimentos_2024_mod.csv'

        # Lê o arquivo transformado diretamente do S3
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()), delimiter=',', header=0, encoding='latin-1')

        df['movimento_liquido_reais'] = (
            df['movimento_liquido_reais']
            .str.replace('.', '', regex=False)  # Remove os separadores de milhar
            .str.replace(',', '.', regex=False)  # Substitui a vírgula decimal por ponto
            .str.replace(r'\((.*?)\)', r'-\1', regex=True)  # Converte (valor) para -valor
            .astype(float)  # Converte para float
        )

        # Conexão ao banco PostgreSQL
        conn = psycopg2.connect(
            host='postgres',
            database='airflow',
            user='airflow',
            password='airflow'
        )
        cursor = conn.cursor()

        # Criar a tabela (caso ainda não exista)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS investimentos (
            ano INT,
            mes VARCHAR(10),
            esfera_orcamentaria INT,
            esfera_orcamentaria_desc VARCHAR(255),
            orgao_maximo INT,
            orgao_maximo_desc VARCHAR(255),
            uo INT,
            uo_desc VARCHAR(255),
            grupo_despesa INT,
            grupo_despesa_desc VARCHAR(255),
            aplicacao INT,
            aplicacao_desc VARCHAR(255),
            resultado INT,
            resultado_desc VARCHAR(255),
            funcao INT,
            funcao_desc VARCHAR(255),
            subfuncao INT,
            subfuncao_desc VARCHAR(255),
            programa INT,
            programa_desc VARCHAR(255),
            acao INT,
            acao_desc VARCHAR(255),
            regiao VARCHAR(50),
            uf VARCHAR(2),
            uf_desc VARCHAR(255),
            municipio VARCHAR(255),
            movimento_liquido_reais NUMERIC
        )
        """)
        conn.commit()

        # Insere os dados no PostgreSQL
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO investimentos (
                    ano, mes, esfera_orcamentaria, esfera_orcamentaria_desc,
                    orgao_maximo, orgao_maximo_desc, uo, uo_desc, grupo_despesa,
                    grupo_despesa_desc, aplicacao, aplicacao_desc, resultado,
                    resultado_desc, funcao, funcao_desc, subfuncao, subfuncao_desc,
                    programa, programa_desc, acao, acao_desc, regiao, uf, uf_desc,
                    municipio, movimento_liquido_reais
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Dados carregados no banco de dados!")


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
