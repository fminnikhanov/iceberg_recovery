# -*- coding: utf-8 -*-
"""
Created on Thu May 23 13:33:38 2019

@author: fminnikhanov
"""
import numpy as np
import dfply as pl
import pandas as pd
import sqlite3

for i in np.arange(len(OrderLogs)):
    df_org=pd.read_csv(file_names[i], header=0)
    for j in np.arange(len(tickers)):
        df=df_org[df_org.SECCODE==tickers[j]]
        df0=df.query('PRICE!=0')
        df0=df>>pl.mutate(s1=np.where((df.ACTION==1)&(df.BUYSELL=="S"), df.VOLUME,0),
                       s2=np.where((df.ACTION==2)&(df.BUYSELL=="S"), df.VOLUME,0),
                       s0=np.where((df.ACTION==0)&(df.BUYSELL=="S"), df.VOLUME,0),
                       b1=np.where((df.ACTION==1)&(df.BUYSELL=="B"), df.VOLUME,0),
                       b2=np.where((df.ACTION==2)&(df.BUYSELL=="B"), df.VOLUME,0),
                       b0=np.where((df.ACTION==0)&(df.BUYSELL=="B"), df.VOLUME,0),
                       timeb=np.where((df.ACTION==2)&(df.BUYSELL=="B"), df.NO-1,0),
                       times=np.where((df.ACTION==2)&(df.BUYSELL=="S"), df.NO-1,0)
                      )>>pl.select(['PRICE','ORDERNO','s1','s2','s0','b1','b2','b0','timeb','times'])
        df0=df0.groupby(['PRICE','ORDERNO']).aggregate({'s1':np.sum,
                                                     's2':np.sum,
                                                     's0':np.sum,
                                                     'b1':np.sum,
                                                     'b2':np.sum,
                                                     'b0':np.sum,
                                                     'timeb':np.max,
                                                     'times':np.max
                                                     }).reset_index(level=["PRICE","ORDERNO"])
        pricecum=df.query('(ACTION==2)&(BUYSELL=="S")&(PRICE!=0)').sort_index(ascending=False).PRICE.cummax().sort_index()
        ind=pricecum.index.values
        dfsc=pd.DataFrame({'PRICEcum': list(pricecum),
                          'times':list(ind)})
        dfs=pd.merge(df0.query("times!=0&s2>=s1"), dfsc, how='left', on="times")
        dfs['sures']=np.where(dfs.PRICEcum>dfs.PRICE,1.0,0.0)
        df0=pd.merge(df0,dfs[['ORDERNO','sures']], how='left', on='ORDERNO')

        pricecum=df.query('(ACTION==2)&(BUYSELL=="B")&(PRICE!=0)').sort_index(ascending=False).PRICE.cummin().sort_index()
        ind=pricecum.index.values
        dfsc=pd.DataFrame({'PRICEcum': list(pricecum),
                          'timeb':list(ind)})
        dfs=pd.merge(df0.query("timeb!=0&b2>=b1"), dfsc, how='left', on="timeb")
        dfs['sureb']=np.where(dfs.PRICEcum<dfs.PRICE,1.0,0.0)
        df0=pd.merge(df0,dfs[['ORDERNO','sureb']], how='left', on='ORDERNO')
        df0['sure']=np.where(((df0.sures==1.0)|(df0.sureb==1.0)),1.0,0.0)
        df0>>=pl.mutate(hids=np.where(df0.s1<df0.s2+df0.s0, df0.s2+df0.s0-df0.s1, 0),
                     hidb=np.where(df0.b1<df0.b2+df0.b0, df0.b2+df0.b0-df0.b1, 0))
        df0>>=pl.mutate(hid=np.where(df0.hids>0, df0.hids, df0.hidb))
        df0=pd.merge(df.query('ACTION==1'), df0[['ORDERNO','hid', 'sure']], how='left', on='ORDERNO')
        df=pd.merge(df, df0[['NO','hid','sure']], how='left', on='NO')
        df["TIMEMLS"]=df.TIME//10000000*60*60*1000-10*60*60*1000+df.TIME%10000000//100000*60000+df.TIME%10000000%100000
        df.to_csv(new_file[i]+tickers[j]+".csv", index=False, header=True)
