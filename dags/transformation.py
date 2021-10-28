from airflow import DAG 
from airflow.operators.python import PythonOperator,BaseOperator
from datetime import datetime,timedelta,date
import pandas as pd
import numpy as np
import os 
import paramiko
import pyodbc
from sqlalchemy import create_engine
from shareplum import Site,Office365
from shareplum.site import Version
import psycopg2
import glob
import csv 
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
import sys
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.message import EmailMessage
from email.mime.base import MIMEBase 
from email import encoders 
from email.mime.multipart import MIMEMultipart 





AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')



def add_columns():
    try:
        df =  pd.read_excel(AIRFLOW_HOME + '/dags/data/merge.xls')
        print(df)

        data_list = []
        amount = 12
        for i in df['makrs']:
            temp = i * amount
            print(temp)
            data_list.append(temp)
        print(data_list)
        df['multiplication'] = data_list
        print(df) 

    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['shindesushant818@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('transformation',default_args=default_args,start_date=datetime.datetime(2021,10,26),schedule_interval="@daily",description='Sharepoint Dag', catchup=False,tags=['example']) as dag:
    add_column = PythonOperator(
        task_id='add_column',
        python_callable=add_columns
    )
