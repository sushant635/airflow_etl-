from __future__ import print_function
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 10, 14),
    'end_date': datetime(2021, 10, 15)    
}
dag = DAG('example_variables', 
    schedule_interval="@once", 
    default_args=default_args)

start = DummyOperator(
    task_id="start",
    dag=dag
)

# if col1.upper() == 'STRING':
#             col1 = str
#         else:
#             col1 = 'error'
#         if col2.upper() == 'STRING':
#             col2 = str
#         else:
#             col2 = 'error'
#         if col3.upper() == 'STRING':
#             col3 = str
#         else:
#             col3 = 'error'
#         if col4.upper() == 'INTGER':
#             col4 = int()
#         else:
#             col4 = 'error'
#         if col5.upper() == 'DATETIME':
#             col5 = date
#         elif col5.upper() == 'DATE':
#             col5 = date
#         else:
#             col5 = 'error'
#         if col6.upper() == 'STRING':
#             col6 = str
#         else:
#             col6 = 'error'
#         if col7.upper() == 'STRING':
#             col7 = str
#         else:
#             col7 = 'error'
#         if col8.upper() == 'STRING':
#             col8 = str
#         else:
#             col8 = 'error'
#         if col9.upper() == 'INTGER':
#             col9 = int()
#         else:
#             col9 = 'error'
#         if col10.upper() == 'INTGER':
#             col10 = int()
#         else:
#             col10 = 'error'
#         if col11.upper() == 'DATETIME':
#             col11 = date
#         elif col11.upper() == 'DATE':
#             col11 = date
#         else:
#             col11 = 'error'

            
#         type_issue_str = []
#         type_count = 0
#         for i in df['Type']:
#             type_count += 1
#             if type(i) == col1:
#                 pass
#             else:
#                 type_issue_str.append(type_count)
#                 print('data type are not same',type_count)

        
#         code_issue_str = []
#         code_count = 0
#         for i in df['Code']:
#             code_count += 1
#             if type(i) == col2:
#                 pass
#             else:
#                 type_issue_str.append(code_count)
#                 print('data type are not same',code_count)

        
#         sales_type_issue_str = []
#         sales_type_count = 0
#         for i in df['Sales Type']:
#             sales_type_count += 1
#             if type(i) == col2:
#                 pass
#             else:
#                 sales_type_issue_str.append(sales_type_count)
#                 print('data type are not same',sales_type_count)


#         sales_type_issue_str = []
#         sales_type_count = 0
#         for i in df['Sales Type']:
#             sales_type_count += 1
#             if type(i) == col3:
#                 pass
#             else:
#                 sales_type_issue_str.append(sales_type_count)
#                 print('data type are not same',sales_type_count )



#         sales_code_str = []
#         sales_code_issue = 0
#         for i in df['Sales Code']:
#             sales_code_issue += 1
#             if type(i) == col4:
#                 pass
#             else:
#                 sales_code_str.append(sales_code_issue)


        
#         starting_date_str = []
#         starting_date_issue = 0
#         for i in df['Starting Date']:
#             if type(i) == str:
#                 temp = i
#             elif type(i) == float:
#                 temp = i
#             else:
#                 temp = i.date()
#             starting_date_issue += 1
#             if type(temp) is col5:
#                 pass
#             else:
#                 starting_date_str.append(starting_date_issue)

#         currency_code_str = []
#         currency_code_issue = 0
#         for i in df['Currency Code']:
#             currency_code_issue += 1
#             if type(i) == col6:
#                 pass
#             else:
#                 currency_code_str.append(currency_code_issue)

#         variant_code_str = []
#         variant_code_issue = 0
#         for i in df['Variant Code']:
#             variant_code_issue += 1
#             if type(i) == col7:
#                 pass
#             else:
#                 variant_code_str.append(variant_code_issue)


#         measure_code_str = []
#         measure_code_issue = 0
#         for i in df['Unit of Measure Code']:
#             measure_code_issue += 1
#             if type(i) == col8:
#                 pass    
#             else:
#                 measure_code_str.append(measure_code_issue)

#         minimum_quantity_str = []
#         minimum_quantity_issue = 0
#         for i in df['Minimum Quantity']:
#             minimum_quantity_issue += 1
#             if type(i) == col9:
#                 pass    
#             else:
#                 minimum_quantity_str.append(minimum_quantity_issue)


#         discount_str = []
#         discount_issue = 0
#         for i in df['Line Discount %']:
#             discount_issue += 1
#             if type(i) == col10:
#                 pass    
#             else:
#                 discount_str.append(discount_issue)
#         ending_date_str = []
#         ending_date_issue = 0
#         for i in df['Ending Date']:
#             if type(i) == str:
#                 temp = i
#             elif type(i) == float:
#                 temp = i
#             else:
#                 temp = i.date()
#             ending_date_issue += 1
#             if type(temp) is col11:
#                 pass
#             else:
#                 ending_date_str.append(ending_date_issue)

dag_config = Variable.get("example_variable", deserialize_json=True)
var1 = dag_config["var1"]
var2 = dag_config["var2"]
var3 = dag_config["var3"]


t1 = BashOperator(
    task_id='get_dag_config',
    bash_command='echo "{0}"'.format(dag_config),
    dag=dag
)

t2 = BashOperator(
    task_id='get_variable_value',
    bash_command='echo {{var.value.val3}}',
    dag=dag
)

t3 = BashOperator(
    task_id = 'get_variable_json',
    bash_command = 'echo {{var.json.example_variable}}',
    dag=dag
)

[t1,t2,t3]
