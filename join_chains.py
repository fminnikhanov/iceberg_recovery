# -*- coding: utf-8 -*-
"""
Created on Thu May 23 13:37:57 2019

@author: fminnikhanov
"""

import numpy as np
import dfply as pl
import pandas as pd
import sqlite3

for reestr in new_file:
    nnn=path2+reestr
    df=pd.read_csv(nnn,header=0)

    if df.TIMEMLS.values[0]>9999999:
        df["TIME"]=df.TIME.values/1000
        df["TIMEMLS"]=df.TIME//10000000*60*60*1000-10*60*60*1000+df.TIME%10000000//100000*60000+df.TIME%10000000%100000
    hidorders=df.loc[(df.hid>0)&(df.ACTION==1),:].ORDERNO.values
    while len(hidorders!=0):
        temp=df.loc[(df.ORDERNO==hidorders[0])&(df.ACTION==1),("BUYSELL","PRICE","VOLUME")].values
        bs=temp[0][0]
        p=temp[0][1]
        v=temp[0][2]
        df1=df.loc[df.ORDERNO.isin(df.loc[(df.BUYSELL==bs)&(df.PRICE==p)&(df.VOLUME==v)&(df.ACTION==1),"ORDERNO"].values),:].copy()
        df1["VOL"]=np.where(df1.ACTION==1,df1.VOLUME+df1.hid,-df1.VOLUME)
        df1['cumvol']=np.cumsum(df1.VOL)
        df1['actch']=np.append(np.nan,df1.ACTION.values[1:]-df1.ACTION.values[:-1])
        df1["cumvolpr"]=np.append(np.nan,df1.cumvol.values[:-1])
        df1["timech"]=np.append(np.nan,df1.TIMEMLS.values[1:]-df1.TIMEMLS.values[:-1])
        orderomit=df1.loc[(df1.ACTION==1)&(df1.cumvolpr>0),"ORDERNO"].values
        if hidorders[0] in orderomit:
            df1=df1.loc[~df1.ORDERNO.isin(df1.loc[df1.NO<df1.loc[(df1.ORDERNO==hidorders[0])&(df1.ACTION==1),:].NO.values[0]].ORDERNO.values),:]
            df1['cumvol']=np.cumsum(df1.VOL)
            df1['actch']=np.append(np.nan,df1.ACTION.values[1:]-df1.ACTION.values[:-1])
            df1["cumvolpr"]=np.append(np.nan,df1.cumvol.values[:-1])
            df1["timech"]=np.append(np.nan,df1.TIMEMLS.values[1:]-df1.TIMEMLS.values[:-1])
            orderomit=df1.loc[(df1.ACTION==1)&(df1.cumvolpr>0),"ORDERNO"].values
            #print("???",orderomit)
        df1=df1.loc[~df1.ORDERNO.isin(orderomit),:]
        df1['cumvol']=np.cumsum(df1.VOL)
        df1['actch']=np.append(np.nan,df1.ACTION.values[1:]-df1.ACTION.values[:-1])
        df1["timech"]=np.append(np.nan,df1.TIMEMLS.values[1:]-df1.TIMEMLS.values[:-1])
        df1['actpr']=np.append(np.nan,df1.ACTION.values[:-1])
        df1["stop"]=np.where((df1.actpr==0)|((df1.ACTION==1)&(df1.timech>12)),1,0)
        df1['stopcum']=np.cumsum(df1.stop)
        stoppoint=df1.loc[df1.ORDERNO==hidorders[0],:].stopcum.values[0]
        df1=df1.loc[df1.stopcum==stoppoint,:]

        if len(np.unique(df1.ORDERNO.values))==1:
            hidorders=hidorders[hidorders!=np.unique(df1.ORDERNO.values)]
            #print(hidorders[0], "DONE")
        else:
            lastaction=df1.ACTION.values[-1]
            lasttime=df1.TIMEMLS.values[-1]
            if lastaction==0:
                orderaddemit=np.array(())
            else:
                orderaddemit=df.loc[(df.BUYSELL==bs)&(df.PRICE==p)&(df.VOLUME<v)&(df.TIMEMLS<lasttime+12)&(df.ACTION==1)&(df.hid<1)&(df.TIMEMLS>=lasttime),:].ORDERNO.values
            if orderaddemit.size==0:
                voladd=0
            else:
                orderaddemit=orderaddemit[0]
                voladd=df.loc[(df.ORDERNO==orderaddemit)&(df.ACTION==1),:].VOLUME.values[0]
            vol=df1.loc[df1.ACTION==1,:].VOL.sum()+voladd
            orderkeep=np.unique(df1.ORDERNO.values)[0]
            orderemit2=np.unique(df1.ORDERNO.values)[1:]
            orderemit2=np.append(orderemit2,orderaddemit)
            df=df.loc[~(df.ORDERNO.isin(orderemit2)&(df.ACTION==1)),:]
            df.loc[df.ORDERNO.isin(orderemit2),"ORDERNO"]=orderkeep
            df.loc[df.NO==df1.NO.values[0],"hid"]=vol-v
            orderemit1=np.append(np.unique(df1.loc[df1.hid>0,"ORDERNO"].values),orderaddemit)
            hidorders=hidorders[[hidorders[i] not in orderemit1 for i in np.arange(len(hidorders))]]
            #print(orderemit1,"Done")
            #print(orderaddemit)

    df.to_csv(path_dfs+new_file2, index=False, header=True)