import time 
from datetime import datetime
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
import numpy as np
import pandas as pd
import sqlalchemy
import subprocess
def ovs_calc():
    exec(open('ovs_calc.py').read())
with DAG(
dag_id='CHANDLER_ANALYTICS_PIPELINE',
schedule_interval='@daily',
default_args={'owner':'airflow','retries':1,
    # 'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023,5,5),},catchup=False) as dag:
    OVS_ETL = PythonOperator(
        task_id='OVS_ETL_DAG',
        python_callable=ovs_calc)
