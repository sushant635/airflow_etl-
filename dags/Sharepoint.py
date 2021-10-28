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

sender = 'sushantshinde549@gmail.com'
receivers = ['shindesushant818@gmail.com']



def sharepoint_collection():
    try:
        username = 'sushantshinde@orientindia.net'
        password = 'Welcome@9979'
        sharepoint_url = 'https://orienttechnologies.sharepoint.com'
        sharepoint_site = 'https://orienttechnologies.sharepoint.com/sites/sushant_ETL'
        sharepoint_doc = 'https://orienttechnologies.sharepoint.com/:x:/s/QuikHr/EamixkIFFFBPi9m_QrWJrZEB7CqltE1eYw1Cj6sEe99Mcw?e=H6ujJW'
        authcookie = Office365(sharepoint_url,username=username, password=password).GetCookies()
        site = Site(sharepoint_site,version=Version.v365,authcookie=authcookie)
        folder = site.Folder('Shared Documents/collection')
        allfiles= folder.files
        index = 0
        for i in allfiles:
            index += 1
            filename = i['Name']
            data = folder.get_file(filename)
            filepath = 'file'+ str(index)+'.xls'
            with open(AIRFLOW_HOME+'/dags/excel_file/'+filepath,'wb') as output_file:
                output_file.write(data)
            print('file path',filepath)
            df = pd.read_excel(AIRFLOW_HOME+'/dags/excel_file/'+filepath)
            df['file_name'] = filename
            print(df)
            df.to_excel(AIRFLOW_HOME+'/dags/excel_file/'+filepath,index=False)
        all_data = pd.DataFrame()
        for f in glob.glob(AIRFLOW_HOME + "/dags/excel_file/file*.xls"):
            df = pd.read_excel(f)
            all_data = all_data.append(df,ignore_index=True)
            print(f)
            file_path = f
            if os.path.exists(file_path):
                # removing the file using the os.remove() method
                os.remove(file_path)
            else:
                # file not found message
                print("File not found in the directory")
            
        all_data.to_excel(AIRFLOW_HOME + '/dags/data/merge.xls',index=False)
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))


def check_datatype_collection():
    try:
        datatype = Variable.get("collection_variables", deserialize_json=True)
        print(datatype.keys())
        col1 = datatype['first_name']
        col2 = datatype['startdate']
        col3 = datatype['makrs']
        # print(datatype)
        # print(len(datatype))
        if col1.upper() == 'STRING':
            col1 = str
        else:
            col1 = 'error'
        if col2.upper() == 'DATETIME':
            col2 = date
        elif col2.upper() == 'DATE':
            col2 = date
        else:
            col2 = 'error'
        if col3.upper() == 'INTGER':
            col3 = int()
        else:
            col3 = 'error'
        df =  pd.read_excel(AIRFLOW_HOME + '/dags/data/merge.xls')
        # df = df.drop_duplicates()
        # df = df.dropna(axis =1,how='all')
        # df = df.dropna(axis = 0, how ='all')
        header_list = df.columns.to_list()
        
        count = 0
        header = []
        check_datatype = []
        for i in header_list:
            if i == 'file_name':
                pass
            else:
                temp = list(datatype.keys())[count]
                values1 = list(datatype.values())[count]
                if values1.upper() == 'STRING':
                    values1 = str
                elif values1.upper() == 'DATETIME':
                    values1 = datetime.datetime
                elif values1.upper() == 'DATE':
                    values1 = datetime.datetime 
                elif values1.upper() == 'INTEGER':
                    values1 = int
                elif values1.upper() == 'FLOAT':
                    values1 = float
                else:
                    values1 = 'error'
                if i == temp:
                    # filename = ''
                    # for file_name in df['file_name']:
                    #     filename = file_name
                    #     print(file_name)
                    columns_row_number = []
                    name_count = 0
                    formate = []
                    file_name_list = []
                    for j,file_name in zip(df[i],df['file_name']):
                        if values1 is datetime.datetime:
                            if type(j) == str:
                                # print('working bdsf')
                                date_format = '%Y-%m-%d'
                                try:
                                    date_obj = datetime.datetime.strptime(j, date_format)
                                except ValueError:
                                    date_obj = str 
                                    formate.append(name_count)
                                    print('Incorrect data format, should be YYYY-MM-DD',name_count,j)
                            else:
                                date_format = '%Y-%m-%d'
                                try:
                                    date_obj = j.strftime(date_format)
                                    date_obj = datetime.datetime.strptime(date_obj,date_format)
                                except ValueError:
                                    date_obj = str 
                                    formate.append(name_count)
                                    print('Incorrect date format, should be YYYY-MM-DD',name_count,j)
                            if type(date_obj) is values1:
                                pass
                            else:
                                print('yefy',file_name)
                                file_name_list.append(file_name)
                                columns_row_number.append({'row number':name_count})
                        else:
                            if type(j) == values1:
                                pass
                            else:
                                print('striuwev',file_name)
                                file_name_list.append(file_name)
                                columns_row_number.append({'row number':name_count})
                        name_count += 1
                    check_datatype.append({i:columns_row_number,'file name':file_name_list,'Incorrect date format':formate})
                else:
                    header.append([i,temp])
                count+= 1

        
        listToStr = ','.join([str(elem) for elem in check_datatype])
        # print(type(listToStr))
        # print(listToStr)
        header_str = ','.join([str(elem) for elem in header])
        header_str = ''.join(str(x) for x in header_str)
        with open(AIRFLOW_HOME + '/dags/data/collection_text.txt', "a") as file_object:
            file_object.write(listToStr)
        
        print('kjb',header_str,type(header_str))
        print(len(header_str))
        if header_str != "":
            message = "Header are not Matching  :" + header_str
        else:
             message = ""
        # print(type(message))
        msg = MIMEMultipart()
        msg.attach(MIMEText(message, 'plain'))
        filename = 'collection_error.txt'
        attachment = open(AIRFLOW_HOME + '/dags/data/collection_text.txt', "rb")
        p = MIMEBase('application', 'octet-stream') 
        p.set_payload((attachment).read())
        encoders.encode_base64(p)
        p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        msg.attach(p)
        text = msg.as_string() 
        msg['Subject'] = 'Collection alert for error'
        msg['From'] = 'sushantshinde549@gmail.com'
        msg['To'] = 'shindesushant818@gmail.com'
        # msg.set_content(message)
        # msg.add_alternative(message, subtype='html')
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login('sushantshinde549@gmail.com', 'oovllsniavlhtvio')
            smtp.send_message(msg)




        

        # proxy = None
        # ssh = paramiko.SSHClient()
        # ssh.load_system_host_keys()
        # ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())            
        # ssh.connect(hostname='192.168.4.24',username='admin',password='th0ughtSp0t')       
        # sftp_client = ssh.open_sftp()
        # print(sftp_client)

        # sftp_client.put(AIRFLOW_HOME + '/dags/data/merge.xls','/home/admin/sushant_collection.csv')
        
            
        

    except Exception as e:
       print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno)) 

def sharepoint_sales():
    try:
        username = 'sushantshinde@orientindia.net'
        password = 'Welcome@9979'
        sharepoint_url = 'https://orienttechnologies.sharepoint.com'
        sharepoint_site = 'https://orienttechnologies.sharepoint.com/sites/sushant_ETL'
        sharepoint_doc = 'https://orienttechnologies.sharepoint.com/:x:/s/QuikHr/EamixkIFFFBPi9m_QrWJrZEB7CqltE1eYw1Cj6sEe99Mcw?e=H6ujJW'
        authcookie = Office365(sharepoint_url,username=username, password=password).GetCookies()
        site = Site(sharepoint_site,version=Version.v365,authcookie=authcookie)
        folder = site.Folder('Shared Documents/sales')
        allfiles= folder.files
        index = 0
        for i in allfiles:
            index += 1
            filename = i['Name']
            data = folder.get_file(filename)
            filepath = 'file'+ str(index)+'.xlsx'
            with open(AIRFLOW_HOME+'/dags/excel_file/'+filepath,'wb') as output_file:
                output_file.write(data)
            df = pd.read_excel(AIRFLOW_HOME+'/dags/excel_file/'+filepath,engine='openpyxl')
            df['file_name'] = filename
            print(df)
            print(df.dtypes)
            df.to_excel(AIRFLOW_HOME+'/dags/excel_file/'+filepath,index=False)
        all_data = pd.DataFrame()
        for f in glob.glob(AIRFLOW_HOME + "/dags/excel_file/file*.xlsx"):
            df = pd.read_excel(f,engine='openpyxl')
            all_data = all_data.append(df,ignore_index=True)
            print(f)
            file_path = f
            if os.path.exists(file_path):
                # removing the file using the os.remove() method
                os.remove(file_path)
            else:
                # file not found message
                print("File not found in the directory")
        all_data.to_excel(AIRFLOW_HOME + '/dags/data/merge_sale.xls',index=False)
    except Exception as e:
        print(e,'error of line number {}'.format(sys.exc_info()[-1].tb_lineno))


def check_datatype_sales():
    try:
        datatype = Variable.get("sales_variables", deserialize_json=True)
        

        df =  pd.read_excel(AIRFLOW_HOME + '/dags/data/merge_sale.xls')
        df = df.replace('',np.nan,regex=True)
        # df = df.dropna(axis =1,how='all')
        # df = df.dropna(axis = 0, how ='all')

        header_list = df.columns.to_list()
        count = 0
        header = []
        check_datatype = []
        var = int
        # for i in df['Sales Code']:
        #     if type(i) is var:
        #         print('working')
        #     else:
        #         print('workign')
        # print(df.dtypes)
        for i in header_list:
            if i == 'file_name':
                pass
            else:
                temp = list(datatype.keys())[count]
                values1 = list(datatype.values())[count]
                if values1.upper() == 'STRING':
                    values1 = str
                elif values1.upper() == 'DATETIME':
                    values1 = datetime.datetime
                elif values1.upper() == 'DATE':
                    values1 = datetime.datetime
                elif values1.upper() == 'INTEGER':
                    values1 = int
                elif values1.upper() == 'FLOAT':
                    values1 = float
                else:
                    values1 = 'error'
                # print(values1)

                if i == temp:
                    columns_row_number = []
                    name_count = 0
                    formate = []
                    file_name_list = []
                    for j,file_name in zip(df[i],df['file_name']):
                        if values1 is datetime.datetime:
                            # print('workings')
                            if type(j) == str:
                                # print('working bdsf')
                                date_format = '%d/%m/%Y'
                                try:
                                    date_obj = datetime.datetime.strptime(j, date_format)
                                except ValueError:
                                    date_obj = str 
                                    formate.append([name_count,j])
                                    print('Incorrect data format, should be YYYY-MM-DD',name_count,j)
                            else:
                                date_format = '%d/%m/%Y'
                                try:
                                    date_obj = j.strftime(date_format)
                                    date_obj = datetime.datetime.strptime(date_obj, date_format)
                                except ValueError:
                                    date_obj = str 
                                    formate.append([name_count,j])
                                    print('Incorrect date format, should be YYYY-MM-DD',name_count,j)
                            if type(date_obj) is values1:
                                pass
                            else:
                                file_name_list.append(file_name)
                                # print(name_count,i)
                                columns_row_number.append({'row number':name_count})
                        else:
                            if type(j) is values1:
                                pass
                            else:
                                # print(i,'ubsdyfv',type(j))
                                file_name_list.append(file_name)
                                columns_row_number.append({'row number':name_count})
                        name_count += 1
                    check_datatype.append({i:columns_row_number,'file name':file_name_list,'Incorrect date format':formate})
                else:
                    header.append([i,temp])
                count+= 1
            # for i in check_datatype:
            #     print(i)
        
        
        
        listToStr = ','.join([str(elem) for elem in check_datatype])
        # print(type(listToStr))
        # print(listToStr)
        header_str = ','.join([str(elem) for elem in header])
        header_str = ''.join(str(x) for x in header_str)
        with open(AIRFLOW_HOME + '/dags/data/sales_text13.txt', "a") as file_object:
            file_object.write(listToStr)
        
        print('kjb',header_str,type(header_str))
        print(len(header_str))
        if header_str != "":
            message = "Header are not Matching  :" + header_str
        else:
             message = ""
        # print(type(message))
        msg = MIMEMultipart()
        msg.attach(MIMEText(message, 'plain'))
        filename = 'sales_error.txt'
        attachment = open(AIRFLOW_HOME + '/dags/data/sales_text13.txt', "rb")
        p = MIMEBase('application', 'octet-stream') 
        p.set_payload((attachment).read())
        encoders.encode_base64(p)
        p.add_header('Content-Disposition', "attachment; filename= %s" % filename)
        msg.attach(p)
        text = msg.as_string() 
        msg['Subject'] = 'Sales alert for error'
        msg['From'] = 'sushantshinde549@gmail.com'
        msg['To'] = 'shindesushant818@gmail.com'
        # msg.set_content(message)
        # msg.add_alternative(message, subtype='html')
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login('sushantshinde549@gmail.com', 'oovllsniavlhtvio')
            smtp.send_message(msg)

        
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

with DAG('sharepoint',default_args=default_args,start_date=datetime.datetime(2021,10,26
),schedule_interval="@daily",description='Sharepoint Dag', catchup=False,tags=['example']) as dag:
    sharepoint_colletion = PythonOperator(
        task_id='sharepoint_collection',
        python_callable=sharepoint_collection
    )
    datatype_collection = PythonOperator(
        task_id = 'datatype_collection',
        python_callable = check_datatype_collection,
        email=['shindesushant818@gmail.com'],
        email_on_failure=True,

    )
    sharepoint_sales = PythonOperator(
        task_id='sharepoint_sales',
        python_callable=sharepoint_sales
    )
    datatype_sales = PythonOperator(
        task_id = 'datatype_sales',
        python_callable = check_datatype_sales
    )
        
    sharepoint_colletion >> datatype_collection >> sharepoint_sales >> datatype_sales
    

