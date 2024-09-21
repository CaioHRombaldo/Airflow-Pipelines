from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta

import awswrangler as wr
import pandas as pd
import requests
import boto3


default_args = {
    'owner': 'Caio Rombaldo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='rawg_upload_dag',
    default_args=default_args,
    schedule_interval="30 14 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def fetch_rawg_data():
        AWS_ACCESS_KEY = Variable.get('AWS_ACCESS_KEY', None)
        AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY', None)
        BUCKET_LANDING = Variable.get('BUCKET_LANDING', 'pydiscovery-landing-423623835158')

        if not AWS_ACCESS_KEY or not AWS_SECRET_ACCESS_KEY:
            return Exception('Não foi possível obter as credenciais de comunicação com a AWS.')
        
        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY, 
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='us-east-1'
        )
        
        rawg_key = Variable.get('RAWG_API_KEY')
        url = f"https://api.rawg.io/api/games?key={rawg_key}"

        response = requests.get(url)
        data = response.json()['results']
        df = pd.DataFrame(data)
        
        wr.s3.to_csv(
            df=df,
            path=f's3://{BUCKET_LANDING}/rawg_data.csv',
            boto3_session=session,
            index=True
        )

    fetch_rawg_data()
