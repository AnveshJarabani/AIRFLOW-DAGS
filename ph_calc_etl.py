import pandas as pd
import numpy as np
from time import time
import h5py,re,os,warnings
warnings.simplefilter(action='ignore', category=(FutureWarning,UserWarning))
os.environ['PYDEVD_DISABLE_FILE_VALIDATION'] = '1'
import multiprocessing as mp
def remove_outliers(group):
    q1 = group['ACT MAT COST'].quantile(0.25)
    q3 = group['ACT MAT COST'].quantile(0.75)
    iqr = q3 - q1
    return group[(group['ACT MAT COST'] >= q1 - 1.5*iqr) & (group['ACT MAT COST'] <= q3 + 1.5*iqr)]
if __name__=='__main__':
    QS=[i for i in h5py.File('PH_RAW.H5').keys() if i.isnumeric()]
    QS = sorted(QS, key=lambda x: (int(re.search(r'(\d{4}$)', x).group()), int(re.search(r'(^\d{2})', x).group())))
    QS_VEN=[i+'_VEN' for i in QS][::-1]
    PH_DICT={i:pd.read_hdf('PH_RAW.H5',key=i) for i in QS}
    for i in PH_DICT.items():
        i[1]['Q+YR']=i[0]        
    # ---------------QUARTERLY TRENDS FOR CHARTS ---------------------
    FL=pd.concat(PH_DICT.values(),keys=PH_DICT.keys(),ignore_index=True)
    PH=FL[['Q+YR','Material - Key','PO Converted Price (Avg)']]
    PH=PH.drop_duplicates(ignore_index=True)
    FR = pd.read_hdf('ST_BM_BR.H5',key="FRAMES")
    def removeframes(X):
                    Y = X.merge(FR,left_on=X.columns[0],right_on='PART NUMBER',how='left')
                    Y = Y.loc[Y['PART NUMBER'].isna()]
                    Y = Y.iloc[:,:3]
                    return Y
    PH=removeframes(PH)
    PH.columns=['Q+YR','PN','BUY COST']
    PH=PH.loc[PH.iloc[:,1].notna()].reset_index(drop=True)
    PH['BUY COST']=PH['BUY COST'].str.replace("$",'')
    PH['BUY COST']=PH['BUY COST'].str.replace(",",'').astype(float)
    PH['LAST Q COST'] = PH.groupby('PN')['BUY COST'].shift(-1)
    PH['DELTA %']=(PH['BUY COST']-PH['LAST Q COST'])/PH['LAST Q COST']
    PH[['BUY COST','DELTA %']]=PH[['BUY COST','DELTA %']].round(2)
    PH['DELTA %'].replace(np.nan,0,inplace=True)
    PH.dropna(how='all',inplace=True)
    PH.to_hdf('PH.H5',key='TREND',mode='w') # DONT PUT BREAKPOINTS ON THIS CODE! PH KEYS WILL BE LOST!
    print('PH.H5 TREND COMPLETE')
    #------------------PURCHASE HISTORY PH FOR CALCULATIONS -------------------
    PH=pd.concat(list(PH_DICT.values())[-5:],ignore_index=True)
    PH=PH[['Material - Key','PO Converted Price (Avg)']]
    PH.columns = ['PH','ACT MAT COST']
    PH['ACT MAT COST']=PH['ACT MAT COST'].str.replace("$",'')
    PH['ACT MAT COST']=PH['ACT MAT COST'].str.replace(",",'').astype(float)
    PH_MATERIALGROUP=PH.groupby('PH')
    pool=mp.Pool(processes=mp.cpu_count())
    result=pool.map(remove_outliers,[df for _,df in PH_MATERIALGROUP])
    PH=pd.concat(result).reset_index(drop=True)
    PH=PH.pivot_table(index=['PH'],values=['ACT MAT COST'],aggfunc=np.average)
    PH.reset_index(inplace=True)
    MERCURY_PART=pd.read_hdf('CY_ADJ.H5',key='MERCURY')
    MERCURY_PART.columns=['PART NUMBER']
    FR=pd.concat([FR,MERCURY_PART])
    def removeframes(X):
                    X.columns = ['PH','ACT MAT COST']
                    Y = X.merge(FR,left_on='PH',right_on='PART NUMBER',how='left')
                    Y = Y.loc[Y['PART NUMBER'].isna()]
                    Y = Y.loc[:,['PH','ACT MAT COST']]
                    return Y
    PH=removeframes(PH)
    PH.loc[PH['PH']=='CY-198172','ACT MAT COST']=18634.5
    PH.to_hdf('PH.H5',key='PH',mode='a')
    print('LBR.H5 PH COMPLETE')
    #------------------------VENDOR DETAILS ------------
    PH_DICT_VEN={i:pd.read_hdf('PH_RAW.H5',key=i) for i in QS_VEN}
    PH=pd.concat(list(PH_DICT_VEN.values())[-10:],ignore_index=True)
    PH.drop_duplicates(subset=['Material - Key'],ignore_index=True,inplace=True)
    def removeframes(X):
                    X.columns = ['PN','LAST VENDOR']
                    Y = X.merge(FR,left_on='PN',right_on='PART NUMBER',how='left')
                    Y = Y.loc[Y['PART NUMBER'].isna()]
                    Y = Y.loc[:,['PN','LAST VENDOR']]
                    return Y
    PH=removeframes(PH)
    PH.to_hdf('PH.H5',key='LST_VENDOR',mode='a')
    print('LBR.H5 LST_VENDOR COMPLETE')