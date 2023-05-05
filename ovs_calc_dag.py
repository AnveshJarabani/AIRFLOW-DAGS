import time 
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base_hook import BaseHook
import numpy as np
import pandas as pd
import sqlalchemy
@task()
def ovs_calc():
    hook=MySqlHook(mysql_conn_id="uct_data")
    QS=hook.get_pandas_df("select * from `qly ints")
    OVS_RAW=hook.get_pandas_df("select * from `ovs_raw`")
    def sort_QS(DF):
        pi=DF.merge(QS,left_on='Q+YR',right_on='Q+YR',how='left')
        pi.sort_values(by=['YR','MONTH'],ascending=True,inplace=True)
        pi=pi[DF.columns]
        pi.drop_duplicates(inplace=True,ignore_index=True)
        return pi
    OVS_RAW = pd.read_pickle('OVS_RAW.PKL')
    OVS = OVS_RAW.loc[~OVS_RAW['PO Amount'].isna()]
    OVS=OVS.loc[~OVS['Operation Description'].str.contains('QN',na=False)]
    OVS['YR']=OVS['Fiscal year / period'].str.extract(r'(\d{2})$')
    OVS['MONTH']=OVS['Fiscal year / period'].str.extract(r'\b(\d{2})\b')
    OVS=OVS.loc[OVS['MONTH'].notna()].reset_index()
    OVS['MONTH']=OVS['MONTH'].astype('int')
    OVS.loc[(OVS['MONTH']==1)|(OVS['MONTH']==2)|(OVS['MONTH']==3),'QTR']='Q1'
    OVS.loc[(OVS['MONTH']==4)|(OVS['MONTH']==5)|(OVS['MONTH']==6),'QTR']='Q2'
    OVS.loc[(OVS['MONTH']==7)|(OVS['MONTH']==8)|(OVS['MONTH']==9),'QTR']='Q3'
    OVS.loc[(OVS['MONTH']==10)|(OVS['MONTH']==11)|(OVS['MONTH']==12),'QTR']='Q4'
    OVS.sort_values(by=['YR','MONTH'],ascending=False,inplace=True)
    OVS['Q+YR']= OVS['QTR'].astype(str)+" "+OVS['YR'].astype(str)
    OVS = OVS.pivot_table(index=['Q+YR','OVS Material - Key', 'OVS Operation'],values=['PO Price'], aggfunc=np.mean)
    OVS.reset_index(inplace=True)
    OVS=OVS.loc[~OVS['OVS Operation'].str.contains('Not',na=False)]
    OVS['OVS Operation']=OVS['OVS Operation'].astype(float)
    OVS['OVS Operation']=OVS['OVS Operation'].astype(int)
    ROUT=pd.read_hdf('ST_BM_BR.H5',key='ROUT')
    ROUT=ROUT.loc[ROUT['Standard Text Key'].str.contains('^21-',regex=True,na=False)]
    OVS=pd.merge(ROUT,OVS,how='left',left_on=['Material','Operation Number'],right_on=['OVS Material - Key','OVS Operation'])
    OVS=OVS.loc[OVS['OVS Operation'].notna()]
    OVS = OVS.pivot_table(index=['Q+YR','OVS Material - Key'], values=['PO Price'], aggfunc=np.sum)
    OVS.reset_index(inplace=True)
    OVS = OVS.loc[OVS['OVS Material - Key'] != '#']
    OVS.rename(columns={'OVS Material - Key': 'MATERIAL', 'PO Price' : 'OVS COST'},inplace=True)
    OVS['OVS COST']=OVS['OVS COST'].round(2)
    OVS=sort_QS(OVS)
    for i in OVS['MATERIAL'].unique():
        OVS.loc[OVS['MATERIAL']==i,'LAST Q COST']=OVS.loc[OVS['MATERIAL']==i,'OVS COST'].shift(1)
    OVS['DELTA %']=(OVS['OVS COST']-OVS['LAST Q COST'])/OVS['LAST Q COST']
    OVS[['OVS COST','DELTA %']]=OVS[['OVS COST','DELTA %']].round(2)
    OVS['DELTA %'].replace(np.nan,0,inplace=True)
    OVS.dropna(how='all',inplace=True)
    OVS.replace([np.inf,-np.inf],np.nan,inplace=True)
    cn_leet=BaseHook.get_connection('leetcode')
    connection_string = f'mysql+pymysql://{cn_leet.login}:{cn_leet.password}@{cn_leet.host}:{cn_leet.port}/{cn_leet.schema}'
    cn_leet.execute('DROP TABLE IF EXISTS OVS_TREND')
    OVS.to_sql(name='OVS_TREND',con=cn_leet,if_exists='replace',index=False)
    for i in OVS.iloc[:,1].unique():
        OVS.loc[OVS.iloc[:,1]==i,'TEMP']=np.roll(OVS.loc[OVS.iloc[:,1]==i,'OVS COST'],1)
    OVS=OVS.loc[OVS.iloc[:,2]<OVS.iloc[:,3]*10]
    OVS=OVS.iloc[:,1:3]
    OVS=OVS.drop_duplicates(subset=['MATERIAL'],keep='last',ignore_index=True)
    cn_leet.execute('DROP TABLE IF EXISTS OVS')
    OVS.to_sql(name='OVS',con=cn_leet,if_exists='replace',index=False) # OVS COST FOR USING IN QUOTE CALCULATION
    print('OVS to sql COMPLETE')
    with DAG(
    dag_id='ovs_trends_calculation',
    schedule_interval='@daily',
    default_args={
        'owner':'airflow',
        'retries':1,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2023,5,5),
    },
    catchup=False) as f:
        ovs_calc = PythonOperator(
            task_id='ovs_cal',
            python_callable=ovs_calc)