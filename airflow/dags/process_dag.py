from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import os

SRC_PATH = Path(__file__).parents[3] / "src"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'log_level': 'INFO',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 1, 1),
}

dag = DAG(
    'space-x-etl-dag',
    default_args=default_args,
    description='A DAG to extract space-x launches and cores data daily and publish it in a db',
    schedule_interval='@daily',
    catchup=False,  # When active, it should do backfill
    concurrency=1,
)

START_DATE = "{{ dag_run.conf.get('start_date',macros.ds_add(ds, -1) )}}"
END_DATE = "{{ dag_run.conf.get('end_date', ds ) }}"


def run_extractor(start_date=None, end_date=None):
    os.system(f"python {SRC_PATH}/extractor.py --start_date {start_date} --end_date {end_date}")

def run_transformer():
    os.system(f"python {SRC_PATH}/transformer.py")

def run_loader(start_date=None, end_date=None):
    os.system(f"python {SRC_PATH}/loader.py --start_date {start_date} --end_date {end_date}")

extractor_task = PythonOperator(
    task_id='extract_data',
    python_callable=run_extractor,
    op_kwargs={'start_date': START_DATE,
               'end_date': END_DATE},
    dag=dag,
)

transformer_task = PythonOperator(
    task_id='transform_data',
    python_callable=run_transformer,
    dag=dag,
)

loader_task = PythonOperator(
    task_id='load_data',
    python_callable=run_loader,
    op_kwargs={'start_date': START_DATE,
               'end_date': END_DATE},
    dag=dag,
)

extractor_task >> transformer_task >> loader_task