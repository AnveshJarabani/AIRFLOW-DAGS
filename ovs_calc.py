from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
import numpy as np
import pandas as pd
import sqlalchemy
# def ovs_pipe():
hook=MySqlHook(mysql_conn_id="uct_data")
b_hook=BaseHook.get_connection('uct_data')
QS=hook.get_pandas_df("select * from `qly ints`")
OVS_RAW=hook.get_pandas_df("select * from `ovs_raw`")
def sort_QS(DF,QS):
    pi=DF.merge(QS,left_on='Q+YR',right_on='Q+YR',how='left')
    pi.sort_values(by=['YR','MONTH'],ascending=True,inplace=True)
    pi=pi[DF.columns]
    pi.drop_duplicates(inplace=True,ignore_index=True)
    return pi
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
ROUT=hook.get_pandas_df("select * from `st_bm_br_rout`")
ROUT=ROUT.loc[ROUT['Standard Text Key'].str.contains('^21-',regex=True,na=False)]
OVS=pd.merge(ROUT,OVS,how='left',left_on=['Material','Operation Number'],right_on=['OVS Material - Key','OVS Operation'])
OVS=OVS.loc[OVS['OVS Operation'].notna()]
OVS = OVS.pivot_table(index=['Q+YR','OVS Material - Key'], values=['PO Price'], aggfunc=np.sum)
OVS.reset_index(inplace=True)
OVS = OVS.loc[OVS['OVS Material - Key'] != '#']
OVS.rename(columns={'OVS Material - Key': 'MATERIAL', 'PO Price' : 'OVS COST'},inplace=True)
OVS['OVS COST']=OVS['OVS COST'].round(2)
OVS=sort_QS(OVS,QS)
for i in OVS['MATERIAL'].unique():
    OVS.loc[OVS['MATERIAL']==i,'LAST Q COST']=OVS.loc[OVS['MATERIAL']==i,'OVS COST'].shift(1)
OVS['DELTA %']=(OVS['OVS COST']-OVS['LAST Q COST'])/OVS['LAST Q COST']
OVS[['OVS COST','DELTA %']]=OVS[['OVS COST','DELTA %']].round(2)
OVS['DELTA %'].replace(np.nan,0,inplace=True)
OVS.dropna(how='all',inplace=True)
OVS.replace([np.inf,-np.inf],np.nan,inplace=True)
connection_string = f'mysql+pymysql://{b_hook.login}:{b_hook.password}@{b_hook.host}:{b_hook.port}/{b_hook.schema}'
hook_engine=sqlalchemy.create_engine(connection_string,
connect_args={'ssl_ca':'/home/anveshjarabani/airflow/dags/DigiCertGlobalRootCA.crt.pem'})
try:
    connection = hook_engine.connect()
    print("Connection successful!")
    connection.close()
except Exception as e:
    print("Connection failed: ", e)
hook_engine.execute('DROP TABLE IF EXISTS OVS_TREND')
OVS.to_sql(name='ovs_trend',con=hook_engine,if_exists='replace',index=False)
print('OVS_TREND ETL COMPLETE')
for i in OVS.iloc[:,1].unique():
    OVS.loc[OVS.iloc[:,1]==i,'TEMP']=np.roll(OVS.loc[OVS.iloc[:,1]==i,'OVS COST'],1)
OVS=OVS.loc[OVS.iloc[:,2]<OVS.iloc[:,3]*10]
OVS=OVS.iloc[:,1:3]
OVS=OVS.drop_duplicates(subset=['MATERIAL'],keep='last',ignore_index=True)
hook_engine.execute('DROP TABLE IF EXISTS OVS')
OVS.to_sql(name='ovs',con=hook_engine,if_exists='replace',index=False) # OVS COST FOR USING IN QUOTE CALCULATION
print('OVS ETL COMPLETE')
