from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta

import os
import awswrangler as wr
import boto3
import logging


default_args = {
    'owner': 'Caio Rombaldo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

LOCAL_DIR = '/temp'

with DAG(
    dag_id='ecommerce_upload_dag',
    default_args=default_args,
    schedule_interval="0 14 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def download_dataset():
        logging.info("Baixando dataset do Kaggle...")
        os.system(f'mkdir -p {LOCAL_DIR}')
        os.system(f'kaggle datasets download -d benroshan/ecommerce-data -p {LOCAL_DIR} --unzip')
        logging.info("Download completo!")

    @task
    def upload_to_s3():
        AWS_ACCESS_KEY = Variable.get('AWS_ACCESS_KEY', None)
        AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY', None)
        BUCKET_LANDING = Variable.get('BUCKET_LANDING', 'pydiscovery-landing-423623835158')
        dt_etl = datetime.now().strftime(r"%Y-%m-%d")

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY, 
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='us-east-1'
        )

        files = ['List of Orders.csv', 'Order Details.csv', 'Sales target.csv']

        for file in files:
            file_path = os.path.join(LOCAL_DIR, file)
            s3_path = f's3://{BUCKET_LANDING}/ecommerce/{dt_etl}/{file}'
            wr.s3.upload(local_file=file_path, path=s3_path, boto3_session=session)
            logging.info(f"{file} enviado para {s3_path}")

    @task
    def cleanup_temp_files():
        logging.info("Removendo arquivos temporários...")
        os.system(f'rm -rf {LOCAL_DIR}')
        logging.info("Arquivos temporários removidos.")

    download_dataset() >> upload_to_s3() >> cleanup_temp_files()
