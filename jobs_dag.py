from airflow.operators.python import PythonOperator
from airflow import DAG
import datetime

from jobs_etl import everyday_job_search_raw,raw_data_to_csv,api_key

default_args = {
    'start_date': datetime(2023, 6, 6), 
    'catchup': False  
}

with DAG('job_search', default_args=default_args,schedule_interval='@daily') as dag:
    task1 = PythonOperator(
        task_id='get_raw_data',
        python_callable=everyday_job_search_raw,
        op_kwargs={'api_key':api_key},
        dag=dag
    )

    task2 = PythonOperator(
        task_id = 'data_to_csv',
        python_callable=raw_data_to_csv,
        op_kwargs={'job_result':task1.output},
        dag=dag
    )

    task1 >> task2
