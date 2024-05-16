import uuid

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
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2020, 1, 1),
}

dag = DAG(
    'space-x-etl-dag-2',
    default_args=default_args,
    description='A DAG to extract space-x launches and cores data daily and publish it in a db',
    schedule_interval='@daily',
    catchup=True,  # When active, it should do backfill
    concurrency=0,
)

START_DATE = "{{ dag_run.conf.get('start_date',macros.ds_add(ds, -1) )}}"
END_DATE = "{{ dag_run.conf.get('end_date', ds ) }}"


def generate_random(**kwargs):
    random_str = str(uuid.uuid4()[:5])
    kwargs['ti'].xcom_push(key='random_str', value=random_str)



def run_extractor(**kwargs):
    random_str = kwargs['ti'].xcom_pull('random_str')
    os.system(f"python {SRC_PATH}/extractor.py --start_date {kwargs['start_date']} --end_date {kwargs['end_date']}"
              f" --random_str {random_str}")

def run_transformer(**kwargs):
    random_str = kwargs['ti'].xcom_pull('random_str')
    os.system(f"python {SRC_PATH}/transformer.py --start_date {kwargs['start_date']} --end_date {kwargs['end_date']}")

def run_loader(**kwargs):
    random_str = kwargs['ti'].xcom_pull('random_str')
    os.system(f"python {SRC_PATH}/loader.py --start_date {kwargs['start_date']} --end_date {kwargs['end_date']}")


generate_ti_task = PythonOperator(
    task_id='generate_ti',
    python_callable=generate_random,
    provide_context=True,
    dag=dag,
)

extractor_task = PythonOperator(
    task_id='extract_data',
    python_callable=run_extractor,
    op_kwargs={'start_date': START_DATE, 'end_date': END_DATE},
    provide_context=True,
    dag=dag,
)

transformer_task = PythonOperator(
    task_id='transform_data',
    python_callable=run_transformer,
    provide_context=True,
    dag=dag,
)

loader_task = PythonOperator(
    task_id='load_data',
    python_callable=run_loader,
    provide_context=True,
    op_kwargs={'start_date': START_DATE, 'end_date': END_DATE},
    dag=dag,
)

generate_ti_task >> extractor_task >> transformer_task >> loader_task