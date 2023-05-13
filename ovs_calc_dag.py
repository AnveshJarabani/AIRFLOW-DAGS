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
def ovs_calc():
    exec(open('ovs_calc.py').read())
def process_calc():
    exec(open('process_days_calc.py').read())
def wc_load_calc():
    exec(open('wc_load_hrs_calc.py').read())
def bom_config():
    print('bom_config complete')
def quote_tables():
    print('quote_tables')
def labor_pnl():
    print('labor_pnl')
def wo_trends():
    print('wo_trends complete')
def ph_calc():
    print('ph_trends complete')
def CY_pnl():
    print('cy_pnl complete')
def LM_pnl():
    print('LM_pnl complete')
def KLA_pnl():
    print('KLA_pnl complete')
with DAG(
dag_id='CHANDLER_ANALYTICS_PIPELINE',
schedule_interval='@daily',
default_args={'owner':'airflow','retries':1,
    # 'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023,5,5),},catchup=False) as dag:
    OVS_ETL = PythonOperator(
        task_id='OVS_ETL_DAG',
        python_callable=ovs_calc)
    PROCESS_DAYS_ETL = PythonOperator(
        task_id='PROCESS_DAYS_ETL_DAG',
        python_callable=process_calc)
    WC_LOAD_ETL = PythonOperator(
        task_id='WC_LOAD_ETL_DAG',
        python_callable=wc_load_calc)
    BOM_CONFIG_ETL=PythonOperator(
        task_id='BOM_CONFIG_DAG',
        python_callable=bom_config)
    QUOTE_TABLES_ETL=PythonOperator(
        task_id='QUOTE_TABLES_DAG',
        python_callable=quote_tables)
    LABOR_PNL_ETL=PythonOperator(
        task_id='LABOR_PNL_DAG',
        python_callable=labor_pnl)
    WO_TRENDS_ETL=PythonOperator(
        task_id='WO_TRENDS_DAG',
        python_callable=labor_pnl)
    PH_TRENDS_ETL=PythonOperator(
        task_id='PH_TRENDS_DAG',
        python_callable=ph_calc)
    CY_PNL_ETL=PythonOperator(
        task_id='CY_PNL_DAG',
        python_callable=CY_pnl)
    LM_PNL_ETL=PythonOperator(
        task_id='LM_PNL_DAG',
        python_callable=LM_pnl)
    KLA_PNL_ETL=PythonOperator(
        task_id='KLA_PNL_DAG',
        python_callable=KLA_pnl)
    BOM_CONFIG_ETL >> QUOTE_TABLES_ETL >> OVS_ETL >> PH_TRENDS_ETL

    PH_TRENDS_ETL >> [PROCESS_DAYS_ETL, WC_LOAD_ETL, LABOR_PNL_ETL, WO_TRENDS_ETL]

    WO_TRENDS_ETL >> [CY_PNL_ETL, LM_PNL_ETL, KLA_PNL_ETL]
