from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta
import awswrangler as wr
import pandas as pd
import boto3
import logging

default_args = {
    'owner': 'Caio Rombaldo',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ecommerce_ingestion_pipeline_enriched',
    default_args=default_args,
    schedule_interval="0 15 * * *",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    @task
    def process_raw_layer():
        AWS_ACCESS_KEY = Variable.get('AWS_ACCESS_KEY', None)
        AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY', None)
        BUCKET_LANDING = Variable.get('BUCKET_LANDING', 'pydiscovery-landing-423623835158')
        BUCKET_RAW = Variable.get('BUCKET_RAW', 'pydiscovery-raw-423623835158')
        dt_etl = datetime.now().strftime(r"%Y-%m-%d")

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='us-east-1'
        )

        df_orders = wr.s3.read_csv(f's3://{BUCKET_LANDING}/ecommerce/{dt_etl}/List of Orders.csv', boto3_session=session)
        df_details = wr.s3.read_csv(f's3://{BUCKET_LANDING}/ecommerce/{dt_etl}/Order Details.csv', boto3_session=session)
        df_sales = wr.s3.read_csv(f's3://{BUCKET_LANDING}/ecommerce/{dt_etl}/Sales target.csv', boto3_session=session)

        # Limpeza básica dos dados
        df_orders_cleaned = df_orders.dropna()
        df_details_cleaned = df_details.dropna()
        df_sales_cleaned = df_sales.dropna()

        # Salvando dados limpos na camada Raw
        wr.s3.to_parquet(df=df_orders_cleaned, path=f's3://{BUCKET_RAW}/ecommerce/{dt_etl}/list_of_orders.parquet', boto3_session=session)
        wr.s3.to_parquet(df=df_details_cleaned, path=f's3://{BUCKET_RAW}/ecommerce/{dt_etl}/order_details.parquet', boto3_session=session)
        wr.s3.to_parquet(df=df_sales_cleaned, path=f's3://{BUCKET_RAW}/ecommerce/{dt_etl}/sales_target.parquet', boto3_session=session)
        logging.info("Dados limpos (raw) enviados para o S3 com sucesso.")

    @task
    def process_integration_layer():
        AWS_ACCESS_KEY = Variable.get('AWS_ACCESS_KEY', None)
        AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY', None)
        BUCKET_RAW = Variable.get('BUCKET_RAW', 'pydiscovery-raw-423623835158')
        BUCKET_INTEGRATION = Variable.get('BUCKET_INTEGRATION', 'pydiscovery-integration-423623835158')
        dt_etl = datetime.now().strftime(r"%Y-%m-%d")

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='us-east-1'
        )

        # Carregar os dados da camada raw
        df_orders = wr.s3.read_parquet(f's3://{BUCKET_RAW}/ecommerce/{dt_etl}/list_of_orders.parquet', boto3_session=session)
        df_details = wr.s3.read_parquet(f's3://{BUCKET_RAW}/ecommerce/{dt_etl}/order_details.parquet', boto3_session=session)

        # Combinar dados de pedidos e detalhes dos pedidos
        df_combined = pd.merge(df_orders, df_details, on='Order_ID')

        df_target = wr.s3.read_parquet(f's3://{BUCKET_RAW}/ecommerce/{dt_etl}/sales_target.parquet', boto3_session=session)

        # Salvando na camada integration
        
        wr.s3.to_parquet(df=df_combined, path=f's3://{BUCKET_INTEGRATION}/ecommerce/combined_data.parquet', boto3_session=session)
        wr.s3.to_parquet(df=df_target, path=f's3://{BUCKET_INTEGRATION}/ecommerce/sales_target.parquet', boto3_session=session)
        logging.info("Dados integrados enviados para o S3 com sucesso.")

    @task
    def process_consume_layer():
        AWS_ACCESS_KEY = Variable.get('AWS_ACCESS_KEY', None)
        AWS_SECRET_ACCESS_KEY = Variable.get('AWS_SECRET_ACCESS_KEY', None)
        BUCKET_INTEGRATION = Variable.get('BUCKET_INTEGRATION', 'pydiscovery-integration-423623835158')
        BUCKET_CONSUME = Variable.get('BUCKET_CONSUME', 'pydiscovery-consume-423623835158')

        session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name='us-east-1'
        )

        # Carregar os dados integrados
        df_combined = wr.s3.read_parquet(f's3://{BUCKET_INTEGRATION}/ecommerce/combined_data.parquet', boto3_session=session)

        # Certificar-se de que a coluna 'Order Date' está no formato datetime
        df_combined['Order_Date'] = pd.to_datetime(df_combined['Order_Date'], errors='coerce')

        # 1. Análise de Vendas por Mês e Categoria
        df_sales_category = df_combined.groupby([pd.Grouper(key='Order_Date', freq='M'), 'Category']).agg({
            'Quantity': 'sum',
            'Profit': 'sum'
        }).reset_index()

        # 2. Produtos mais vendidos (Top 10)
        df_top_products = df_combined.groupby('Sub-Category')['Quantity'].sum().sort_values(ascending=False).head(10).reset_index()

        # 3. Margem de Lucro por Categoria
        df_profit_margin = df_combined.groupby('Category').agg({
            'Amount': 'sum',
            'Profit': 'sum'
        }).reset_index()
        df_profit_margin['Profit Margin'] = df_profit_margin['Profit'] / df_profit_margin['Amount']

        # 4. Sazonalidade (Vendas por Trimestre)
        df_sales_trimestral = df_combined.groupby(pd.Grouper(key='Order_Date', freq='Q')).agg({
            'Quantity': 'sum',
            'Profit': 'mean'
        }).reset_index()

        # 5. Análise de Desempenho vs Metas
        df_sales_target = wr.s3.read_parquet(f's3://{BUCKET_INTEGRATION}/ecommerce/sales_target.parquet', boto3_session=session)

        # Converter a coluna 'Month_of_Order_Date' em datetime no formato correto
        df_sales_target['Month_of_Order_Date'] = pd.to_datetime(df_sales_target['Month_of_Order_Date'], format='%b-%y')
        
        df_target_analysis = pd.merge(df_sales_category, df_sales_target, how='inner', left_on=['Order_Date', 'Category'], right_on=['Month_of_Order_Date', 'Category'])

        df_target_analysis['Performance'] = df_target_analysis['Profit'] - df_target_analysis['Target']

        # Salvando os insights na camada consume
        dt_etl = datetime.now().strftime(r"%Y-%m-%d")

        # 1. Salvando a análise de vendas por mês e categoria
        wr.s3.to_parquet(
            df=df_sales_category,
            path=f's3://{BUCKET_CONSUME}/ecommerce/{dt_etl}/sales_category.parquet',
            boto3_session=session
        )

        # 2. Salvando os produtos mais vendidos (Top 10)
        wr.s3.to_parquet(
            df=df_top_products,
            path=f's3://{BUCKET_CONSUME}/ecommerce/{dt_etl}/top_10_products.parquet',
            boto3_session=session
        )

        # 3. Salvando a margem de lucro por categoria
        wr.s3.to_parquet(
            df=df_profit_margin,
            path=f's3://{BUCKET_CONSUME}/ecommerce/{dt_etl}/profit_margin.parquet',
            boto3_session=session
        )

        # 4. Salvando a análise de sazonalidade (vendas por trimestre)
        wr.s3.to_parquet(
            df=df_sales_trimestral,
            path=f's3://{BUCKET_CONSUME}/ecommerce/{dt_etl}/sales_trimestral.parquet',
            boto3_session=session
        )

        # 5. Salvando a análise de desempenho vs metas
        wr.s3.to_parquet(
            df=df_target_analysis,
            path=f's3://{BUCKET_CONSUME}/ecommerce/{dt_etl}/target_analysis.parquet',
            boto3_session=session
        )

        logging.info("Todos os insights foram gerados e enviados para o S3 com sucesso.")

    # Fluxo de tarefas do DAG
    raw_layer = process_raw_layer()
    integrated_layer = process_integration_layer()
    consume_layer = process_consume_layer()

    # Definir ordem de execução das tarefas
    raw_layer >> integrated_layer >> consume_layer
