from airflow import DAG 
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime,timedelta
import pandas as pd 
import numpy as np
import os

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def data():
    data = [['Alex',10],['Bob',12],['Clarke',13]]
    df = pd.DataFrame(data,columns=['Name','Age'])
    df = df.to_json()
    print(df)
    return df



def load_excel():
    try:
        pf = pd.read_excel(AIRFLOW_HOME + '/dags/data/file.xls')
        pf = pf.to_json()
        print(pf)
        return pf 
    except Exception as e:
        print(e)
        return e

def get_listings_data():
    try:
        listings = pd.read_csv(AIRFLOW_HOME + '/dags/data/listing.csv')
        listings = listings.to_json()
        return listings
    except Exception as e:
        print(e)
        return e



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

with DAG('file_upload',default_args=default_args,start_date=datetime(2021,10,1),schedule_interval="@daily",description='A simple tutorial DAG', catchup=False,tags=['example']) as dag:
    data = PythonOperator(
        task_id='data',
        python_callable=data
    )
    file_upload = PythonOperator(
        task_id='file_upload',
        python_callable=load_excel
    )
    csv_load = PythonOperator(
        task_id='csv_load',
        python_callable=get_listings_data
    )

    data >> file_upload >> csv_load
 
