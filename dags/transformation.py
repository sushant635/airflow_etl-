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
import time 



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




def transformation_to_sharepoint():
    try:
        username = 'sushantshinde@orientindia.net'
        password = 'Welcome@9979'
        sharepoint_url = 'https://orienttechnologies.sharepoint.com'
        sharepoint_site = 'https://orienttechnologies.sharepoint.com/sites/sushant_ETL'
        sharepoint_doc = 'https://orienttechnologies.sharepoint.com/:x:/s/QuikHr/EamixkIFFFBPi9m_QrWJrZEB7CqltE1eYw1Cj6sEe99Mcw?e=H6ujJW'
        authcookie = Office365(sharepoint_url,username=username, password=password).GetCookies()
        site = Site(sharepoint_site,version=Version.v365,authcookie=authcookie)
        folder = site.Folder('Shared Documents/tranformations')
        # files = ''
        data = folder.get_file('tranformation.xlsx')
        # c

        # print(data)
        with open(AIRFLOW_HOME+'/dags/excel_file/tranformation.xlsx','wb') as output_file:
                output_file.write(data)

        base_data = pd.read_excel(AIRFLOW_HOME+'/dags/data/tranformation.xls')
        sharepoint_data = pd.read_excel(AIRFLOW_HOME+'/dags/excel_file/tranformation.xlsx',engine='openpyxl')
        # with open(AIRFLOW_HOME+'/dags/excel_file/tranformation.xlsx','rb') as output:
        #     folder.upload_file(output, 'tranformation.xlsx')
        # print(base_data)
        data = pd.merge(base_data,sharepoint_data,on='company_name',how='left', indicator=True)
        base_data['owner'] = data[['owner_y']]
        base_data.to_excel(AIRFLOW_HOME+'/dags/data/tranformation1.xls',index=False)
        # print(base_data)       
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))

def appendvalueintoSharepoint():
    try:
        username = 'sushantshinde@orientindia.net'
        password = 'Welcome@9979'
        sharepoint_url = 'https://orienttechnologies.sharepoint.com'
        sharepoint_site = 'https://orienttechnologies.sharepoint.com/sites/sushant_ETL'
        sharepoint_doc = 'https://orienttechnologies.sharepoint.com/:x:/s/QuikHr/EamixkIFFFBPi9m_QrWJrZEB7CqltE1eYw1Cj6sEe99Mcw?e=H6ujJW'
        authcookie = Office365(sharepoint_url,username=username, password=password).GetCookies()
        site = Site(sharepoint_site,version=Version.v365,authcookie=authcookie)
        folder = site.Folder('Shared Documents/tranformations')
        # files = ''
        data = folder.get_file('tranformation.xlsx')
        # print(data)
        with open(AIRFLOW_HOME+'/dags/excel_file/append.xlsx','wb') as output_file:
            output_file.write(data)
        base_data = pd.read_excel(AIRFLOW_HOME+'/dags/data/tranformation.xls')
        sharepoint_data = pd.read_excel(AIRFLOW_HOME+'/dags/excel_file/append.xlsx',engine='openpyxl')

        # print(sharepoint_data)
        # print(base_data)
        # data = pd.merge(sharepoint_data,base_data,on='company_name',how='left', indicator=True)
        f = base_data[~base_data.company_name.isin(sharepoint_data.company_name)]
        k = list(f.company_name.values)
        data_values = []
        owner_list = []
        for i in sharepoint_data['company_name']:
        #     print(i)
            data_values.append(i)
        for j in sharepoint_data['owner']:
            owner_list.append(j)
        for o in k:
            data_values.append(o)
            owner_list.append(np.nan)
        folder.delete_file('tranformation.xlsx')
        df = pd.DataFrame({'company_name': data_values, 'owner': owner_list})
        print(df)
        df.to_excel(AIRFLOW_HOME+'/dags/data/append1.xlsx',index=False)
        time.sleep(60)
        with open(AIRFLOW_HOME+'/dags/data/append1.xlsx','rb') as output:
            folder.upload_file(output, 'tranformation.xlsx')
        if os.path.exists(AIRFLOW_HOME+'/dags/data/append1.xlsx'):
                # removing the file using the os.remove() method
                os.remove(AIRFLOW_HOME+'/dags/data/append1.xlsx')
        else:
            # file not found message
            print("File not found in the directory")

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

with DAG('transformation',default_args=default_args,start_date=datetime.datetime(2021,10,29),description='Sharepoint Dag', catchup=False,tags=['example']) as dag:
    add_column = PythonOperator(
        task_id='add_column',
        python_callable=add_columns
    )
    tranformation_to_sharepoint = PythonOperator(
        task_id='tranformation_to_sharepoint',
        python_callable=transformation_to_sharepoint
    )
    append_value_sharepoint = PythonOperator(
        task_id='append_value_sharepoint',
        python_callable=appendvalueintoSharepoint
    )

    tranformation_to_sharepoint >> append_value_sharepoint >> add_column