from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.etl_utils import load_csv_smart
from helpers.etl_utils import load_product_info

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'csv_to_postgres_loader',
    default_args=default_args,
    description='Загрузка данных из CSV в PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2025, 7, 15),
    catchup=False,
    tags=['etl'],
) as dag:

    load_deal_info = PythonOperator(
        task_id='load_deal_info',
        python_callable=load_csv_smart,
        op_kwargs={
            'csv_path': '/opt/airflow/data/deal_info.csv',
            'table_name': 'deal_info',
            'schema': 'rd'
        }
    )
    load_product_info = PythonOperator(
        task_id='load_product_info',
        python_callable=load_product_info,
        op_kwargs={
            'csv_path': '/opt/airflow/data/product_info.csv',
            'table_name': 'product',
            'schema': 'rd'
        }
    )

    load_deal_info >> load_product_info
    