from airflow import DAG 
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime,timedelta
import pandas as pd 
import numpy as np
import os
import paramiko
import sys

AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def get_file():
    try:
        proxy = None
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # ssh.connect(hostname = '192.168.20.94',username='ridhimasawant',password='Oct@2020')
        # sftp_client = ssh.open_sftp()
        # sftp_client.get('E:\SalesInvoiceTracking\Sales Invoice Tracking Report_2017-till date.csv','/home/user/workspace/airflow/dags/data/Sales Invoice Tracking Report_2017-till date.csv')
        ssh.connect(hostname = '192.168.4.19',username='tinamenezes',password='apr@2020')
        sftp_client = ssh.open_sftp()
        # sftp_client.get('C:/Users/tinamenezes/Downloads/Sales Discount Remove_30072020_30248.xlsx','/home/user/workspace/airflow/dags/data/Sales Invoice Tracking Report_2017-till date.xlsx')
        sftp_client.get('C:/Users/tinamenezes/Downloads/s1.xlsx','/home/user/workspace/airflow/dags/data/Sales Invoice Tracking Report_2017-till date.xlsx')
        df = pd.read_excel('/home/user/workspace/airflow/dags/data/Sales Invoice Tracking Report_2017-till date.xlsx',engine='openpyxl')
        pf = pd.read_excel(AIRFLOW_HOME+'/dags/data/file.xlsx',engine='openpyxl')
        print(pf,type(pf))
        today = datetime.now()
        df['timestamp'] = today
        if pf.empty:
            pf = df
            pf.to_excel(AIRFLOW_HOME+'/dags/data/file.xlsx',index=False)
        else:
            result = pf.append(df)
            result.to_excel(AIRFLOW_HOME+'/dags/data/file.xlsx',index=False)
        sftp_client.close()
    except paramiko.AuthenticationException:
        print('Failed to connect to %s due to wrong username/password')
        exit(1)
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))


def put_excel_file():
    try:
        proxy = None
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())            
        ssh.connect(hostname='192.168.4.24',username='admin',password='th0ughtSp0t')       
        sftp_client = ssh.open_sftp()
        sftp_client.put('/home/user/workspace/airflow/dags/data/file.xlsx','C:/Users/tinamenezes/Downloads/sushant.xlsx')
        sftp_client.close()
    except paramiko.AuthenticationException:
        print('Failed to connect to %s due to wrong username/password')
        exit(1)
    except Exception as e:
        print(e)





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    
}

with DAG('file_upload',default_args=default_args,start_date=datetime(2021,10,1),schedule_interval="@daily",description='A simple tutorial DAG', catchup=False,tags=['example']) as dag:
    get_file = PythonOperator(
        task_id='get_file',
        python_callable=get_file
    )
    put_excel = PythonOperator(
        task_id='put_excel',
        python_callable=put_excel_file
    )
 
    get_file >> put_excel
