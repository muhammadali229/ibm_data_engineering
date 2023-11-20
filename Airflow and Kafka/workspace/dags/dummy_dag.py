# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Muhammad Ali',
    'start_date': days_ago(0), # means today
    'email': ['ma6627863@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define the DAG
dag = DAG(
    dag_id='dummy_dag',
    default_args=default_args,
    description='Dummy Dag',
    schedule_interval=timedelta(minutes=1),
)

# define the tasks

# define the first task named extract
task_1 = BashOperator(
    task_id='task_1',
    bash_command='sleep 1',
    dag=dag
)
task_2 = BashOperator(
    task_id='task_2',
    bash_command='sleep 2',
    dag=dag
)
task_3 = BashOperator(
    task_id='task_3',
    bash_command='sleep 3',
    dag=dag
)


# task pipeline
task_1 >> task_2 >> task_3
