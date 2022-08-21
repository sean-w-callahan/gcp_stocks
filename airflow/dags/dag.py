from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
#from google.cloud import storage
from datetime import date, datetime, timedelta
import os
from scripts.get_prices import get_prices


start_date = datetime(2022, 8, 18, hour=6, minute=17, second=0, microsecond=0)

api_key = os.environ['api_key']

with DAG(
    #'get_data_and_store',
    'testing_6',
    description = 'Combined DAG', 
    schedule_interval = '16 6 * * *',
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

    t3 = GCSToBigQueryOperator(
        task_id = 'gcs_to_bigquery',
        bucket = 'stocks-data-output',
        source_objects = [f'prices_{yesterday}.csv'],
        destination_project_dataset_table = "main.stock_prices",
        schema_object = 'schema.json',
        # schema_fields = [
        #     {'name': 'ticker', 'type': 'STRING', 'mode': 'NULLABLE'},
        #     {'name': 'volume', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #     {'name': 'vol_weighted_avg_price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #     {'name': 'open', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #     {'name': 'close', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #     {'name': 'high', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #     {'name': 'low', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #     {'name': 'unix_timestamp', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #     {'name': 'num_transactions', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        #     {'name': 'datetime', 'type': 'DATE', 'mode': 'NULLABLE'}
        # ],
        write_disposition='WRITE_APPEND',
    )
    
    t1 >> t2 >> t3