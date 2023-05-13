import pandas as pd
import numpy as np
wl=pd.read_pickle('LBR M-18.PKL')
rout=pd.read_hdf('ST_BM_BR.H5',key='ROUT') 
BURCOST=pd.read_hdf('ST_BM_BR.H5',key='BR') 
BURCOST.dropna(inplace=True,how='all')
wl = wl[(wl['Order - Material (Key)'] != '#') & (wl['Order'] != '#')]
def filteroverheadkeys(x,y):
     x = x.merge(BURCOST,left_on=y,right_on='ST KEY',how='left')
     x.drop(columns=['ST KEY'],axis=1,inplace=True)
     x=x.loc[x['BUR_RATE']!=0]
     return x
wl=filteroverheadkeys(wl,'Standard Text Key')
pi = wl.pivot_table(index=["Order - Material (Key)",
     "Order",'Operation Quantity'],values=["Hours Worked"],aggfunc= np.sum)
pi.reset_index(inplace=True)
pi['HRS/EA'] = pi['Hours Worked']/pi['Operation Quantity']
pi.replace([np.inf, -np.inf], np.nan, inplace=True)
pi.dropna(how='any',inplace=True)
pi.drop_duplicates(subset=['Order'],keep='last',inplace=True)
PERIOD=wl[['Fiscal year/period','Order']]
PERIOD.drop_duplicates(subset=['Order'],keep='last',inplace=True)
PERIOD['key1']=PERIOD['Fiscal year/period'].str.extract(r'((?<=\s)\d{2})')
PERIOD['key2']=PERIOD['Fiscal year/period'].str.extract(r'((?<=\s)\d{4})')
PERIOD.dropna(how='any',inplace=True)
PERIOD[['key1','key2']]=PERIOD[['key1','key2']].astype(float)
PERIOD.sort_values(by=['key2','key1'],ignore_index=True,inplace=True)
PERIOD.drop(columns=['key1','key2'],inplace=True,axis=1)
pk=PERIOD.merge(pi,how='left',on=['Order'],validate='1:1')
pk['HRS/EA']=pk['HRS/EA'].astype(float)
pk['Order']=pk['Order'].astype(str)
pk.drop_duplicates(subset=['Order'],keep='last',inplace=True)
pk.to_hdf('LBR.H5',key='WO_TRENDS',mode='a')
rout.columns=rout.columns.str.strip()
rt=rout.loc[:,['Material','Standard Text Key','Base Quantity','Setup','Unit_Setup','Labor Run','Unit_Labor Run']]
rt=rt.loc[rout['Unit_Setup']!='#']
QTY = wl.pivot_table(index=['Order - Material (Key)'],values=['Operation Quantity'],aggfunc=np.mean)
QTY.reset_index(inplace=True) 
rt = rt.merge(QTY,left_on='Material',right_on='Order - Material (Key)',how='left')
rt.drop(columns=['Order - Material (Key)'],axis=1,inplace=True)
rt.loc[rt['Operation Quantity'].isnull(),'Operation Quantity'] = rt['Base Quantity']
rt['PLN HR/EA'] = ((rt['Setup'] + rt['Labor Run'])/rt['Operation Quantity']).where(rt['Unit_Labor Run'] == 'H',\
                 (rt['Setup'] + (rt['Labor Run']/60))/rt['Operation Quantity'])
rt = rt.merge(BURCOST,left_on='Standard Text Key',right_on='ST KEY',how='left')
rt.drop(columns=['ST KEY'],axis=1,inplace=True)
rt=rt.loc[rt['BUR_RATE']!=0]
rt = rt.pivot_table(index=['Material'],values=['PLN HR/EA'],aggfunc=np.sum)
rt.reset_index(inplace=True)
rt['PLN HR/EA']=rt['PLN HR/EA'].astype(float)
rt.select_dtypes(include=[float]).astype(np.float16)
rt.select_dtypes(include=[int]).astype(np.int8) 
rt.to_hdf('LBR.H5',key='PLN_HR',mode='a')
print('LBR.H5 PLN_HR COMPLETE')