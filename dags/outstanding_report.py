from airflow import DAG 
from airflow.operators.python import PythonOperator , BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd 
import  numpy as np
import os 
import paramiko

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def get_excel_file():
    try:
        proxy = None
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname = '192.168.4.19',username='tinamenezes',password='apr@2020')
        sftp_client = ssh.open_sftp()
        sftp_client.get('C:/NAVLiveReports/OutstandingReport/Orient Live Outstanding Report.xlsx',AIRFLOW_HOME + '/dags/data/Orient Live Outstanding Report.xlsx')
        sftp_client.close()
    except paramiko.AuthenticationException:
        print('Failed to connect to %s due to wrong username/password')
    except Exception as e:
        print(e)



def put_excel_file():
    try:
        proxy = None
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname='192.168.4.24',username='admin',password='th0ughtSp0t')
        sftp_client = ssh.open_sftp()
        sftp_client.put(AIRFLOW_HOME + '/dags/data/Orient Live Outstanding Report.xlsx','/home/admin/Outstanding_Report/Orient Live Outstanding Report.xlsx')
        sftp_client.close()
    except paramiko.AuthenticationException:
        print('Failed to connect to %s due to wrong username/password')
    except Exception as e:
        print(e)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['shindesushant818@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


with DAG('Outstanding_resport',default_args=default_args,start_date=datetime(2021,11,10),schedule_interval="@daily",description='A simple tutorial DAG', catchup=False,tags=['example']) as dag:
    get_excel = PythonOperator(
        task_id='get_excel',
        python_callable=get_excel_file
    )
    put_excel = PythonOperator(
        task_id = 'put_excel',
        python_callable=put_excel_file
    )


    get_excel>>put_excel 