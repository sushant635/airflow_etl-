from airflow import DAG 
from airflow.operators.python import PythonOperator,BranchPythonOperator
from datetime import datetime,timedelta
import paramiko
import sys
import os 



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
        sftp_client.get('C:/Users/tinamenezes/Downloads/Sales Discount Remove_30072020_30248.xlsx','/home/user/workspace/airflow/dags/data/Sales Discount Remove_30072020_30248.xls')
        print(sftp_client)
        sftp_client.close()
    except paramiko.AuthenticationException:
        print('Failed to connect to %s due to wrong username/password')
        exit(1)
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['zeyamozahid@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('file',default_args=default_args,start_date=datetime(2021,10,1),schedule_interval="@daily",description='A simple tutorial DAG', catchup=False,tags=['example']) as dag:
    get_file = PythonOperator(
        task_id='get_file',
        python_callable=get_file
    )
