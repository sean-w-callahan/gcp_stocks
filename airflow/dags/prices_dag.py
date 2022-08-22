from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from datetime import date, timedelta
import os
from scripts.get_prices import get_prices


start_date = date(2022, 8, 15)

api_key = os.environ['api_key']

with DAG(
    #'get_data_and_store',
    'store_data',
    description = 'Gets data, uploads to GCS, moves to Bigquery', 
    schedule_interval = '0 7 * * 2,3,4,5,6',
    start_date = start_date, 
    catchup = False, 
    default_args = {'retries': 1}
) as dag:

    today = date.today()  
    yesterday = str(today - timedelta(days = 1))  

    t1 = PythonOperator(
        task_id = 'run_python_script', 
        python_callable = get_prices,
        op_kwargs = {"api_key" : api_key, "yesterday" : yesterday}
    )
    
    t2 = GCSObjectExistenceSensor(
        task_id = 'gcs_sensor',
        bucket = 'stocks-data-output',
        object= f'prices_{yesterday}.csv',
        mode = 'poke',
        poke_interval = 60,
        timeout = 180
    )

    t3 = BigQueryCheckOperator(
        task_id = 'check_date',
        sql = f'SELECT CAST(MAX(date) AS STRING) != "{yesterday}" FROM main.stock_prices',
        bigquery_conn_id = 'bigquery_default'
    )

    t4 = GCSToBigQueryOperator(
        task_id = 'gcs_to_bigquery',
        bucket = 'stocks-data-output',
        source_objects = [f'prices_{yesterday}.csv'],
        destination_project_dataset_table = "main.stock_prices",
        schema_object = 'schema.json',
        write_disposition='WRITE_APPEND',
    )

    t5 = GCSDeleteObjectsOperator(
        task_id = 'delete_csv',
        bucket_name = 'stocks-data-output',
        objects = [f'prices_{yesterday}.csv'],
        gcp_conn_id='google_cloud_default'
    )
    
    t1 >> t2 >> t3 >> t4 >> t5