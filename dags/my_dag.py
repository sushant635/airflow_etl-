from airflow import DAG 
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint 
from datetime import datetime,timedelta

def choose_best_model(ti):
    accuracies = ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    if max(accuracies) > 8:
        return 'accurate'
    return 'inaccurate'

def training_model():
    print(randint(1,10))
    return randint(1,10)

def list_print():
    for i in range(1,10):
        print(i)
    return 'Hello World'

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

with DAG('my_dag',default_args=default_args,start_date=datetime(2021,10,1),schedule_interval="@daily",description='A simple tutorial DAG', catchup=False,tags=['example']) as dag:
    training_model_A = PythonOperator(
        task_id='training_model_A',
        python_callable=training_model
    )
    training_model_B = PythonOperator(
        task_id='training_model_B',
        python_callable=training_model
    )
    training_model_C = PythonOperator(
        task_id='training_model_C',
        python_callable=training_model
    )

    data_model = PythonOperator(
        task_id='print_list',
        python_callable = list_print
    )

    choose_best_model = BranchPythonOperator(
        task_id = 'choose_best_model',
        python_callable=choose_best_model
    )


    accurate = BashOperator(
            task_id="accurate",
            bash_command="echo 'accurate'"
        )

    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command="echo 'inaccurate'"
    )

    [training_model_A,training_model_B,training_model_C] >> data_model >> choose_best_model >> [accurate,inaccurate]