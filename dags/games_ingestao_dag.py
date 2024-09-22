from pickle import TRUE
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

import pandas as pd
import awswrangler as wr
import boto3


default_args = {
    'owner': 'Caio Rombaldo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='games_ingestao_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    @task
    def process_raw_layer():
        AWS_ACCESS_KEY = Variable.get('AWS_ACCESS_KEY')
        AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
        BUCKET_LANDING = Variable.get('BUCKET_LANDING')
        BUCKET_RAW = Variable.get('BUCKET_RAW')
        dt_etl = datetime.now().strftime(r"%Y-%m-%d")

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY, 
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='us-east-1'
        )

        rawg_df = wr.s3.read_csv(path=f's3://{BUCKET_LANDING}/games/{dt_etl}/rawg_data.csv', boto3_session=session)
        igdb_df = wr.s3.read_csv(path=f's3://{BUCKET_LANDING}/games/{dt_etl}/igdb_data.csv', boto3_session=session)

        # Limpeza inicial dos dados
        rawg_cleaned = rawg_df[['name', 'rating', 'released', 'platforms']]
        igdb_cleaned = igdb_df[['name', 'rating', 'platforms']]

        # Adicionando coluna de dt_etl
        rawg_cleaned['dt_etl'] = dt_etl
        igdb_cleaned['dt_etl'] = dt_etl

        # Salvando os limpos na camada Raw diretamente no S3
        wr.s3.to_parquet(
            df=rawg_cleaned,
            path=f's3://{BUCKET_RAW}/games/{dt_etl}/rawg_data.parquet',
            boto3_session=session,
            index=True
        )
        wr.s3.to_parquet(
            df=igdb_cleaned,
            path=f's3://{BUCKET_RAW}/games/{dt_etl}/igdb_data.parquet',
            boto3_session=session,
            index=True
        )
    
    @task
    def integrate_data():
        AWS_ACCESS_KEY = Variable.get('AWS_ACCESS_KEY')
        AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
        BUCKET_RAW = Variable.get('BUCKET_RAW')
        BUCKET_INTEGRATION = Variable.get('BUCKET_INTEGRATION')
        dt_etl = datetime.now().strftime(r"%Y-%m-%d")

        rawg_cleaned = wr.s3.read_parquet(path=f's3://{BUCKET_RAW}/games/{dt_etl}/rawg_data.parquet', boto3_session=session)
        igdb_cleaned = wr.s3.read_parquet(path=f's3://{BUCKET_RAW}/games/{dt_etl}/igdb_data.parquet', boto3_session=session)

        integrated_df = pd.merge(rawg_cleaned, igdb_cleaned, on="name", how="inner")

        # Salvando os dados integrados na camada Integration diretamente no S3
        wr.s3.to_parquet(
            df=integrated_df,
            path=f's3://{BUCKET_INTEGRATION}/games/integrated_data.parquet',
            boto3_session=boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY, 
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name='us-east-1'
            ),
            index=True
        )

    @task
    def prepare_for_consume():
        AWS_ACCESS_KEY = Variable.get('AWS_ACCESS_KEY')
        AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY')
        BUCKET_INTEGRATION = Variable.get('BUCKET_INTEGRATION')
        BUCKET_CONSUME = Variable.get('BUCKET_CONSUME')

        integrated_df = wr.s3.read_parquet(path=f's3://{BUCKET_INTEGRATION}/games/integrated_data.parquet', boto3_session=session)
        
        # Geração de insights - Exemplo: Média de avaliações por plataforma
        insights = integrated_df.groupby('platforms')['rating'].mean().reset_index()
        
        # Salvando os insights na camada Consume diretamente no S3
        wr.s3.to_parquet(
            df=insights,
            path=f's3://{BUCKET_CONSUME}/games/insights.parquet',
            boto3_session=boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY, 
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name='us-east-1'
            ),
            index=True
        )

    process_raw_layer() >> integrate_data() >> prepare_for_consume()
