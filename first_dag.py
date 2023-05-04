try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    print('all modules ok')
except Exception as e:
    print('error {}'.format(e))
def first_func(*args,**kwargs):
    variable=kwargs.get('name','didnt get the value')
    print('hello world'+variable)
    return 'hello world' +variable

# */2 **** execute every two minutes

with DAG(
    dag_id='first_dag.py',
    schedule='@daily',
    default_args={
        'owner':'airflow',
        'retries':1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2023,3,5),
    },
    catchup=False) as f:
    first_func = PythonOperator(
        task_id='first_func',
        python_callable=first_func,
        op_kwargs={'name':'soumil shah'})