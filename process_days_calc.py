import pandas as pd
import numpy as np
from datetime import datetime,timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.hooks.base import BaseHook
import sqlalchemy
hook=MySqlHook(mysql_conn_id="uct_data")
b_hook=BaseHook.get_connection('uct_data')
RAWLBR=hook.get_pandas_df("select * from `lbr m-18`")
RAWLBR=RAWLBR[['Order - Material (Key)','Order', 'Operation', 'Standard Text Key','End Date' ]]
RAWLBR=RAWLBR.loc[RAWLBR['Order - Material (Key)']!='#']
RAWLBR=RAWLBR.loc[RAWLBR['Order']!='#']
RAWLBR=RAWLBR.loc[RAWLBR['Standard Text Key']!='#']
RAWLBR=RAWLBR.loc[RAWLBR['End Date'].notna()]
RAWLBR['End Date']=RAWLBR['End Date'].astype('datetime64[ns]')
filt_date=RAWLBR['End Date'].max()-timedelta(days=7*30)
RAWLBR=RAWLBR.loc[RAWLBR['End Date']>filt_date]
mins=RAWLBR.pivot_table(index=['Order - Material (Key)','Order','Standard Text Key', 'Operation'],values='End Date',aggfunc=np.min)
mins.reset_index(inplace=True)
mins.rename(columns={'End Date': 'Oldest'},inplace=True)
maxs=RAWLBR.pivot_table(index=['Order - Material (Key)','Order','Standard Text Key', 'Operation'],values='End Date',aggfunc=np.max)
maxs.reset_index(inplace=True)
maxs.rename(columns={'End Date': 'Latest'},inplace=True)
merge=mins.merge(maxs,how='left')
merge['days']=(merge['Latest']-merge["Oldest"]).dt.days
merge.loc[merge['days']==0,'days']=1
merge[['Operation','Order']]=merge[['Operation','Order']].astype(int)
merge.sort_values(by=['Order','Operation'],inplace=True,ignore_index=True)
x=merge.groupby('Order')
merge['WAIT']=(x['Oldest'].shift(0)-x['Latest'].shift(1)).dt.days
merge.loc[merge['WAIT'].isna() | (merge['WAIT']<0),'WAIT']=0
merge=merge.pivot_table(index=['Order - Material (Key)','Standard Text Key','Operation'],values=['days','WAIT'],aggfunc=np.average)
merge.reset_index(inplace=True)
RT=hook.get_pandas_df("select * from `st_bm_br_rout`")
RT=RT[['Material','Operation Number']]
merge['Operation']=merge['Operation'].astype('int64')
RT=RT.merge(merge,left_on=['Material','Operation Number'],right_on=['Order - Material (Key)','Operation'],how='left')
RT['OP']=RT['Standard Text Key']+"("+RT['Operation'].astype(str)+")"
RT=RT[['Material','OP','Operation','days','WAIT']]
RT.columns=['Material','OP','Operation','PROCESS DAYS','WAIT DAYS']
RT.dropna(how='any',inplace=True)
RT=RT.melt(id_vars=['Material','OP','Operation'],
        var_name='PROCESS/WAIT',value_name='DAYS')
RT=RT.sort_values(by='Operation',ignore_index=True)
RT['DAYS']=RT['DAYS'].astype(int)
RT.dropna(subset=['OP'],inplace=True)
connection_string = f'mysql+pymysql://{b_hook.login}:{b_hook.password}@{b_hook.host}:{b_hook.port}/{b_hook.schema}'
hook_engine=sqlalchemy.create_engine(connection_string,
connect_args={'ssl_ca':'/home/anveshjarabani/airflow/dags/DigiCertGlobalRootCA.crt.pem'})
try:
    connection = hook_engine.connect()
    print("Connection successful for process days!")
    connection.close()
except Exception as e:
    print("Connection failed: ", e)
hook_engine.execute('DROP TABLE IF EXISTS lbr_process_dys')
RT.to_sql(name='lbr_process_dys',con=hook_engine,if_exists='replace',index=False)
print('PROCESS DAYS ETL COMPLETE')

