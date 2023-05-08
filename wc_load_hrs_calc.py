import pandas as pd
import numpy as np
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
import sqlalchemy
hook=MySqlHook(mysql_conn_id="uct_data")
b_hook=BaseHook.get_connection('uct_data')
wl=hook.get_pandas_df("select * from `lbr m-18`")
cst=hook.get_pandas_df("select * from `st_bm_br_br`")
wl = wl.loc[:,['Fiscal year/period', 'Order - Material (Key)',
       'Order - Material (Text)', 'Order', 'Operation', 'Work Center',        
       'Standard Text Key', 'Operation Text', 'End Date',
       'Operation Quantity', 'Hours Worked', 'Labor Rate', 'Labor Cost',
       'Overhead Rate', 'Overhead Cost']]
wl = wl.loc[(wl['Order - Material (Key)'] != '#')&(wl['Order'] != '#')&(wl['Standard Text Key'] != '#')]
wl['YR']=wl['Fiscal year/period'].str.extract(r'(\d{2})$')
wl['MONTH']=wl['Fiscal year/period'].str.extract(r'\b(\d{2})\b')
wl['X']=wl['MONTH'].astype(str)+" "+wl['YR'].astype(str)
wl=wl.loc[wl['MONTH'].notna()].reset_index(drop=True)
wl['MONTH']=wl['MONTH'].astype('int')
pi = wl.pivot_table(index=['YR','MONTH','X','Work Center'],values=["Hours Worked"],aggfunc= np.sum)
pi.reset_index(inplace=True)
pi.replace([np.inf, -np.inf], np.nan, inplace=True)
pi.dropna(how='any',inplace=True)
pi2 = pi.pivot_table(index=['Work Center'],values = ['Hours Worked'],aggfunc= np.mean)
pi2.reset_index(inplace=True)
pi2.columns=['WC_mean','MONTHLY AVG. HRS']
pi3=pi.merge(pi2,left_on='Work Center',right_on='WC_mean',how='left')
pi3.drop(columns=['WC_mean'],inplace=True)
cst=cst[['WC','BUR_RATE']].drop_duplicates(ignore_index=True)
pi3 = cst.merge(pi3,left_on='WC',right_on='Work Center',how='left')
pi.sort_values(by=['YR','MONTH'],ascending=False,inplace=True,)
pi3.drop(columns=['YR','MONTH','WC','BUR_RATE'],axis=1,inplace=True)
pi3.dropna(inplace=True)
pi3[['Hours Worked','MONTHLY AVG. HRS']]=pi3[['Hours Worked','MONTHLY AVG. HRS']].round(2)
connection_string = f'mysql+pymysql://{b_hook.login}:{b_hook.password}@{b_hook.host}:{b_hook.port}/{b_hook.schema}'
hook_engine=sqlalchemy.create_engine(connection_string,
connect_args={'ssl_ca':'/home/anveshjarabani/airflow/dags/DigiCertGlobalRootCA.crt.pem'})
try:
    connection = hook_engine.connect()
    print("Connection successful for WC LOAD ETL!")
    connection.close()
except Exception as e:
    print("Connection failed: ", e)
hook_engine.execute('DROP TABLE IF EXISTS lbr_wc_load')
RT.to_sql(name='lbr_wc_load',con=hook_engine,if_exists='replace',index=False)
print('WC LOAD ETL COMPLETE')
