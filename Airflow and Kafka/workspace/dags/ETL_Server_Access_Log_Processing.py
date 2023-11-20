# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import requests
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
    dag_id='ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='Sample ETL DAG using Bash',
    schedule_interval=timedelta(days=1)
)

def download():
    res = requests.get('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt')
    with open('/home/airflow/web-server-access-log.txt', 'wb') as f:
        f.write(res.content)

# define the tasks

# download task
download = PythonOperator(
    task_id='download',
    python_callable=download,
    dag=dag
)

# define the first task named extract
extract = BashOperator(
    task_id='extract',
    bash_command='cut -d"#" -f1,4 /home/airflow/web-server-access-log.txt > /home/airflow/extracted-data_new.txt',
    dag=dag,
)

# define the second task named transform
transform_load = BashOperator(
    task_id='transform',
    bash_command='tr "[a-z]" "[A-Z]" < /home/airflow/extracted-data_new.txt > /home/airflow/capitalized.txt',
    dag=dag,
)


# task pipeline
download >> extract >> transform_load

