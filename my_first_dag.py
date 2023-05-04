from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

dag = DAG(
    dag_id='my_first_dag',
    start_date=datetime(2023, 3, 31),
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
)
task1 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello, Airflow"',
    dag=dag,
)



task1.set_downstream(task2)