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


def get_excel_file():
    try:
        # f = pd.read_excel(open('\\192.168.20.94\SalesInvoiceTracking.xls','rb'))
        proxy = None
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        # ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname = '192.168.4.19',username='tinamenezes',password='apr@2020')
        sftp_client = ssh.open_sftp()
        sftp_client.get('C:/Users/tinamenezes/Downloads/Sales Discount Remove_30072020_30248.xlsx','/home/user/workspace/airflow/dags/data/sales.xlsx')
        print(sftp_client)
        sftp_client.close()
    except paramiko.AuthenticationException:
        print('Failed to connect to %s due to wrong username/password')
        exit(1)
    except Exception as e:
        print(e)


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
        with open(AIRFLOW_HOME+'/dags/data/sql.xlsx','rb') as output:
            folder.upload_file(output, 'sql.xlsx')
        f = df.drop_duplicates()
        print(len(f))
        print(f)
        
        f.to_excel(AIRFLOW_HOME + '/dags/data/sqlremoveduplicate.xlsx',index=False)
        with open(AIRFLOW_HOME+'/dags/data/sqlremoveduplicate.xlsx','rb') as output:
            folder.upload_file(output, 'sqlremoveduplicate.xlsx')
        # host = '192.168.4.19'
        # db_name = 'Orient_2016'
        # username ='tinamenezes'
        # password = 'apr@2020'

        # connection = create_engine('mysql://{username}:{password}@{url}/{db_name}?charset=utf8'.format(username=username, password=password,url=host, db_name=db_name), echo=False)
        # conn = connection.connect()
        # print(conn)
    
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))


def sharepoint_excel():
    try:
        username = 'sushantshinde@orientindia.net'
        password = 'Welcome@9979'
        sharepoint_url = 'https://orienttechnologies.sharepoint.com'
        sharepoint_site = 'https://orienttechnologies.sharepoint.com/sites/QuikHr'
        sharepoint_doc = 'https://orienttechnologies.sharepoint.com/:x:/s/QuikHr/EamixkIFFFBPi9m_QrWJrZEB7CqltE1eYw1Cj6sEe99Mcw?e=H6ujJW'

        authcookie = Office365(sharepoint_url,username=username, password=password).GetCookies()
        site = Site(sharepoint_site,version=Version.v365,authcookie=authcookie)
        folder = site.Folder('Shared Documents/new')
        allfiles= folder.files
        index = 0
        for i in allfiles:
            index += 1
            filename = i['Name']
            data = folder.get_file(filename)
            filepath = 'file'+ str(index)+'.xls'
            with open(AIRFLOW_HOME+'/dags/excel_file/'+filepath,'wb') as output_file:
                output_file.write(data)

        all_data = pd.DataFrame()
        for f in glob.glob(AIRFLOW_HOME + "/dags/excel_file/file*.xls"):
            df = pd.read_excel(f)
            all_data = all_data.append(df,ignore_index=True)

        print(all_data)
        all_data.to_excel(AIRFLOW_HOME + '/dags/data/merge.xlsx')

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
        # data_xls = pd.read_excel(AIRFLOW_HOME + '/dags/data/file.xls', dtype=str, index_col=None)
        # data_xls.to_csv(AIRFLOW_HOME + '/dags/data/csvfile.csv', encoding='utf-8', index=False)
        # print(data_xls)
        sftp_client.put('/home/user/workspace/airflow/dags/data/sushant.csv','/home/admin/sushant.csv')
    except paramiko.AuthenticationException:
        print('Failed to connect to %s due to wrong username/password')
        exit(1)
    except Exception as e:
        print(e)



def connect_to_postgreSQL():
    try:
        host = '192.168.20.25'
        database = 'survey_03_09_2021'
        user = 'odoo13'
        password = 'odoo13'
        
        conn = psycopg2.connect(database=database,user=user,password=password,port= '5432',host='192.168.20.25')
        print(conn)
        cur = conn.cursor()
        cur.execute('SELECT * FROM survey_user_input ')
        title = [i[0] for i in cur.description]
        db_version = cur.fetchall()
        li = []
        li.append(title)
        for i in db_version:
            li.append(i)
        df = pd.DataFrame(li)
        with open(AIRFLOW_HOME + '/dags/data/input2.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for i in li:
                writer.writerow(i)
        # df.to_csv(AIRFLOW_HOME + '/dags/data/input2.csv', encoding='utf-8', index=False)
        
        cur.execute('SELECT * FROM survey_user_input')
        title = [i[0] for i in cur.description]
        input_1 = cur.fetchall()
        input1 = []
        input1.append(title)
        for i in input_1:
            input1.append(i)
        with open(AIRFLOW_HOME + '/dags/data/input.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for i in input1:
                writer.writerow(i)


        cur.execute('SELECT * FROM survey_user_input INNER JOIN   res_partner ON partner_id=res_partner.id')
        title = [i[0] for i in cur.description]
        inner_join =  cur.fetchall()
        # print(inner_join)
        inner_list = []
        inner_list.append(title)
        for i in inner_join:
            # print(i)
            inner_list.append(i)
        # print(inner_list)
        inner = pd.DataFrame(inner_list,index=None,columns=None )
        # inner.reset_index(drop=True,inplace=True)
        # inner.reset_index(inplace=True)
        # inner.drop("index",axis=1,inplace=True) 
        with open(AIRFLOW_HOME + '/dags/data/inner.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for i in inner_list:
                writer.writerow(i)
        # inner.to_csv(AIRFLOW_HOME + '/dags/data/inner.csv',index=False,)
        # inner.to_excel(AIRFLOW_HOME + '/dags/data/inner.xlsx')


        cur.execute('SELECT * FROM survey_user_input LEFT JOIN res_partner ON partner_id=res_partner.id')
        title = [i[0] for i in cur.description]
        left_join = cur.fetchall()
        left_list = []
        left_list.append(title)
        for i in left_join:
            # print(i)
            left_list.append(i)
        left = pd.DataFrame(left_list)
        with open(AIRFLOW_HOME + '/dags/data/left.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for i in left_list:
                writer.writerow(i)
        # left.to_csv(AIRFLOW_HOME + '/dags/data/left.csv', encoding='utf-8', index=True)

        cur.execute('SELECT * FROM survey_user_input RIGHT JOIN   res_partner ON partner_id=res_partner.id')
        title = [i[0] for i in cur.description]
        right_join = cur.fetchall()
        right_list = []
        right_list.append(title)
        for i in right_join:
            right_list.append(i)
        right = pd.DataFrame(right_list)
        with open(AIRFLOW_HOME + '/dags/data/right.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for i in right_list:
                writer.writerow(i)
        # right.to_csv(AIRFLOW_HOME + '/dags/data/right.csv', encoding='utf-8', index=False)

        cur.execute('SELECT * FROM survey_user_input FULL  JOIN survey_survey ON  survey_survey.id=survey_id')
        title = [i[0] for i in cur.description]
        full_join = cur.fetchall()
        full_list = []
        full_list.append(title)
        for i in full_join:
            full_list.append(i)

        full = pd.DataFrame(full_list)
        with open(AIRFLOW_HOME + '/dags/data/full.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            for i in full_list:
                writer.writerow(i)
        # full.to_csv(AIRFLOW_HOME + '/dags/data/full.csv', encoding='utf-8', index=False)

        
        frame = [inner,left,right,full]

        df_keys = pd.concat(frame,keys=['Inner Join','Left Join','Right Join','Full Join'])

        # df_keys.to_csv(AIRFLOW_HOME + '/dags/data/join.csv', encoding='utf-8', index=False)

        # print(df_keys)
        cur.close()

    except Exception as e:
        print(e)

def replace_data():
    try:
        data = pd.read_excel(AIRFLOW_HOME + '/dags/data/file.xls')
        
        print(data)
        # data.email.replace(r'\..{2}$','.com',regex=True)
        # data['email'].str.replace(r'\..{2}$','com')
        # data.replace({'email': r'\..{2}$'},{'email':'com'},regex=True)
        df = pd.DataFrame(data)
        columns_name = input('enter the colum name')
        to_string = input('enter replace to data')
        out_string = input('which string')
        print(df)
        # df.replace('omkumar@orientindia.net','.com',inplace=True)
        # df['email'] = df['email'].str.rsplit('.').str[0] + '.com'
        index = 0
        for i in df[columns_name]:
            print(i)
            my = i
            my_str = str(i)
            print(type(i))
            replase_str = my_str.replace(to_string,out_string)
            
            print('replace string',replase_str,type(replase_str))
            try:
                df[columns_name].replace(my,replase_str,inplace=True)
            except Exception as e:
                print(e)
            print(replase_str)

        print(df['email'])
        print(df)
        # print(df['email'])
        # print(df)

    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))


def remove_duplicates():
    try:
        # host = '192.168.20.25'
        # database = 'survey_03_09_2021'
        # user = 'odoo13'
        # password = 'odoo13'
        
        # conn = psycopg2.connecI am asking for any change need to connect SQL t(database=database,user=user,password=password,port= '5432',host='192.168.20.25')
        # print(conn)
        # cur = conn.cursor()
        # cur.execute('SELECT * FROM survey_user_input')
        # # print( cur.description)
        # title = [i[0] for i in cur.description]
        # db_version = cur.fetchall()
        # li = []
        # li.append(title)
        # for i in db_version:
        #     li.append(i)
        # df = pd.DataFrame(li)
        # f = df.drop_duplicates(subset='e')
        # print(df)
        # print(f)
        data = pd.read_csv(AIRFLOW_HOME +'/dags/data/input2.csv')
        df = pd.DataFrame(data)
        f = df.drop_duplicates()
        print(data)
        print(f)
        # f.to_csv(AIRFLOW_HOME +'/dags/data/remove_duplicate.csv')
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))

def remove_duplicates_column():
    try:
        data = pd.read_csv(AIRFLOW_HOME +'/dags/data/input2.csv')
        df = pd.DataFrame(data)
        print(df)
        z = pd.Series(df['email']).duplicated()
        df['z'] = z
        print(df)
        count = 0
        for i in df['z']:
            if i== True:
                print(count)
                df.loc[count,'email'] = np.nan
            count += 1
        print(df)
        df.drop('z', inplace=True, axis=1)
        print(df)
        df.to_csv(AIRFLOW_HOME +'/dags/data/duplicate.csv')
    
    except Exception as e:
       print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno)) 

def check_blank():
    try:
        data = pd.read_excel(AIRFLOW_HOME + '/dags/data/file.xls')
        print(data)
        df = pd.DataFrame(data)
        # print(df)
        df1 = df.replace('',np.nan,regex=True)
        f = df1.dropna(axis =1,how='all')
        b = f.dropna(axis = 0, how = 'all')
        print(b)
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

with DAG('connectserver',default_args=default_args,start_date=datetime(2021,10,1),schedule_interval="@daily",description='A simple tutorial DAG', catchup=False,tags=['example']) as dag:
    get_excel = PythonOperator(
        task_id='get_excel',
        python_callable=get_excel_file
    )
    put_excel = PythonOperator(
        task_id='put_excel',
        python_callable=put_excel_file
    )
    connect_sql = PythonOperator(
        task_id = 'connect_sql',
        python_callable=connect_sql_server
    )
    sharepoint_excel = PythonOperator(
        task_id='sharepoint_excel',
        python_callable=sharepoint_excel
    )
    connect_postgresql = PythonOperator(
        task_id='connect_postgresql',
        python_callable=connect_to_postgreSQL
    )
    replace_data = PythonOperator(
        task_id = 'replace_data',
        python_callable=replace_data
    )
    remove_duplicates = PythonOperator(
        task_id='remove_duplicates',
        python_callable=remove_duplicates
    )
    remove_duplicates_column = PythonOperator(
        task_id='remove_duplicates_column',
        python_callable=remove_duplicates_column
    )
    check_blank = PythonOperator(
        task_id='check_blank',
        python_callable=check_blank
    )
    email = EmailOperator(
        task_id = 'send_email',
        to = 'zeyamozahid@gmail.com',
        subject = 'Airflow Alert',
        html_content = '<h3>Email Test Airflow </h3>'
    )


    get_excel >> put_excel
