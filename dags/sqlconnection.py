from airflow import DAG 
from airflow.operators.python import PythonOperator,BranchPythonOperator
from datetime import datetime,timedelta
import pandas as pd 
import  numpy as np
import os 
import paramiko
import requests
import pyodbc
from sqlalchemy import create_engine
#import MySQLdb
from shareplum import Site,Office365
from shareplum.site import Version
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
from py_topping.data_connection.sharepoint import da_tran_SP365
from requests.auth import HTTPBasicAuth
import psycopg2
import pymssql
import glob
import sys
import csv 
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator


AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')



def connect_sql_server():
    try:
        driver = "{ODBC Driver 17 for SQL Server}"
        server = 'OTMUMNAVDEMO1,1433'
        database = 'Orient_2016'
        username = 'tinamenezes'
        password = 'apr@2020'
        port = '1433'

        username = 'sushantshinde@orientindia.net'
        password = 'Welcome@9979'
        sharepoint_url = 'https://orienttechnologies.sharepoint.com'
        sharepoint_site = 'https://orienttechnologies.sharepoint.com/sites/sushant_ETL'
        sharepoint_doc = 'https://orienttechnologies.sharepoint.com/:x:/s/QuikHr/EamixkIFFFBPi9m_QrWJrZEB7CqltE1eYw1Cj6sEe99Mcw?e=H6ujJW'
        authcookie = Office365(sharepoint_url,username=username, password=password).GetCookies()
        site = Site(sharepoint_site,version=Version.v365,authcookie=authcookie)
        folder = site.Folder('Shared Documents/tranformations')
        # conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
        #                'Server=192.168.4.19,1433;'                      
        #                'Database=Orient_2016;'                       
        #                'UID=sa;'
        #               'PWD=apr@2020'
        #                'Trusted_Connection=yes;'
        #                )

        conn = pymssql.connect(server='192.168.4.19', user='sa', password='apr@2020', database='Orient_2016')
           
        cursor = conn.cursor()
        print(cursor)


        SQL_Query = pd.read_sql_query(
        '''SELECT [Name],[Search Name] FROM [dbo].[Orient Demo$Customer]''', conn)
        
        df = pd.DataFrame(SQL_Query, columns=['Name', 'Search Name'])
        print(len(df))

        df.to_excel(AIRFLOW_HOME + '/dags/data/sql.xlsx',index=False)
        # with open(AIRFLOW_HOME+'/dags/data/sql.xlsx','rb') as output:
        #     folder.upload_file(output, 'sql.xlsx')
        f = df.drop_duplicates()
        print(len(f))
        # print(f)
        
        f.to_excel(AIRFLOW_HOME + '/dags/data/sqlremoveduplicate.xlsx',index=False)

        data = pd.merge(df,f,on='Name',how='left', indicator=True)
        data.to_excel(AIRFLOW_HOME + '/dags/data/sqlmerge.xlsx',index=False)

        print(data)
        with open(AIRFLOW_HOME+'/dags/data/sqlremoveduplicate.xlsx','rb') as output:
            folder.upload_file(output, 'sql.xlsx')
    
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))


def sql_tranformation():
    try:
        username = 'sushantshinde@orientindia.net'
        password = 'Welcome@9979'
        sharepoint_url = 'https://orienttechnologies.sharepoint.com'
        sharepoint_site = 'https://orienttechnologies.sharepoint.com/sites/sushant_ETL'
        sharepoint_doc = 'https://orienttechnologies.sharepoint.com/:x:/s/QuikHr/EamixkIFFFBPi9m_QrWJrZEB7CqltE1eYw1Cj6sEe99Mcw?e=H6ujJW'
        authcookie = Office365(sharepoint_url,username=username, password=password).GetCookies()
        site = Site(sharepoint_site,version=Version.v365,authcookie=authcookie)
        folder = site.Folder('Shared Documents/tranformations')
        data = folder.get_file('sql.xlsx')
        with open(AIRFLOW_HOME+'/dags/excel_file/tranformation.xlsx','wb') as output_file:
                output_file.write(data)

        sharepoint_data = pd.read_excel(AIRFLOW_HOME+'/dags/excel_file/tranformation.xlsx',engine='openpyxl')
        # print(sharepoint_data)
        conn = pymssql.connect(server='192.168.4.19', user='sa', password='apr@2020', database='Orient_2016')
        SQL_Query = pd.read_sql_query(
        '''SELECT [No_],[Name],[Search Name] FROM [dbo].[Orient Demo$Customer]''', conn)

        df = pd.DataFrame(SQL_Query,columns=['No_','Name', 'Search Name'])
        print(df)
        data = pd.merge(df,sharepoint_data,on='Name',how='left', indicator=True)
        # print(data)
        data.to_excel(AIRFLOW_HOME + '/dags/data/sqlmerge.xlsx',index=False)
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))

def sql_append():
    try:
        username = 'sushantshinde@orientindia.net'
        password = 'Welcome@9979'
        sharepoint_url = 'https://orienttechnologies.sharepoint.com'
        sharepoint_site = 'https://orienttechnologies.sharepoint.com/sites/sushant_ETL'
        sharepoint_doc = 'https://orienttechnologies.sharepoint.com/:x:/s/QuikHr/EamixkIFFFBPi9m_QrWJrZEB7CqltE1eYw1Cj6sEe99Mcw?e=H6ujJW'
        authcookie = Office365(sharepoint_url,username=username, password=password).GetCookies()
        site = Site(sharepoint_site,version=Version.v365,authcookie=authcookie)
        folder = site.Folder('Shared Documents/tranformations')
        data = folder.get_file('sql.xlsx')
        with open(AIRFLOW_HOME+'/dags/excel_file/tranformation.xlsx','wb') as output_file:
                output_file.write(data)

        sharepoint_data = pd.read_excel(AIRFLOW_HOME+'/dags/excel_file/tranformation.xlsx',engine='openpyxl')
        # print(sharepoint_data)
        conn = pymssql.connect(server='192.168.4.19', user='sa', password='apr@2020', database='Orient_2016')
        SQL_Query = pd.read_sql_query(
        '''SELECT [No_],[Name],[Search Name] FROM [dbo].[Orient Demo$Customer]''', conn)

        df = pd.DataFrame(SQL_Query,columns=['No_','Name', 'Search Name'])

        f = df[~df.Name.isin(sharepoint_data.Name)]
        print(f)
        k = list(f.Name.values)
        data_values = []
        owner_list = []
        for i in sharepoint_data['Name']:
            data_values.append(i)
        for j in sharepoint_data['Search Name']:
            owner_list.append(j)
        for o in k:
            data_values.append(o)
            owner_list.append(np.nan)
        pf = pd.DataFrame({'Name': data_values, 'Search Name': owner_list})
        pf.to_excel(AIRFLOW_HOME+'/dags/data/append1.xlsx',index=False)
        with open(AIRFLOW_HOME+'/dags/data/append1.xlsx','rb') as output:
            folder.upload_file(output, 'sql.xlsx')

    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))
        
        # data = pd.merge(df,sharepoint_data,on='Name',how='left', indicator=True)
        # print(data)
        # data.to_excel(AIRFLOW_HOME + '/dags/data/sqlmerge.xlsx',index=False)

def get_file():
    try:
        proxy = None
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname = '192.168.20.94',username='ridhimasawant',password='Oct@2020')
        sftp_client = ssh.open_sftp()
        sftp_client.get('E:\SalesInvoiceTracking\Sales Invoice Tracking Report_2017-till date.csv','/home/user/workspace/airflow/dags/data/Sales Invoice Tracking Report_2017-till date.csv')
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


with DAG('sqlconnection',default_args=default_args,start_date=datetime(2021,10,1),schedule_interval="@daily",description='A simple tutorial DAG', catchup=False,tags=['example']) as dag:
    connect_sql_tra = PythonOperator(
        task_id='connect_sql_tra',
        python_callable=connect_sql_server
    )
    sql_tranformation = PythonOperator(
        task_id='sql_tranformation',
        python_callable = sql_tranformation
    )
    sql_append = PythonOperator(
        task_id='sql_append',
        python_callable=sql_append
    )
    get_file = PythonOperator(
        task_id='get_file',
        python_callable=get_file
    )

