from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

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
    dag_id='igdb_upload_dag',
    default_args=default_args,
    schedule_interval="30 14 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:
    
    @task
    def fetch_igdb_data():
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

        igdb_key = Variable.get('IGDB_API_KEY')
        url = "https://api.igdb.com/v4/games"
        headers = {'Client-ID': 'YOUR_TWITCH_CLIENT_ID', 'Authorization': f'Bearer {igdb_key}'}

        response = requests.post(url, headers=headers, json={"fields": "name,rating,platforms; limit 10;"})
        data = response.json()
        df = pd.DataFrame(data)
        dt_etl = datetime.now().strftime(r"%Y-%m-%d")

        wr.s3.to_csv(
            df=df,
            path=f's3://{BUCKET_LANDING}/games/{dt_etl}/igdb_data.csv',
            boto3_session=session,
            index=True
        )

    fetch_igdb_data()
