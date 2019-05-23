# -*- coding: utf-8 -*-
"""
Created on Thu May 23 13:39:47 2019

@author: fminnikhanov
"""
import pandas as pd
import numpy as np


def bookrec(df0, time, NO=0):
    if type(df0)!=pd.core.frame.DataFrame:
        df0=pd.read_csv(df0, header=0)
    if NO==0:
        df=df0.loc[(df0.PRICE!=0)&(df0.TIMEMLS<=time),:].copy()
    else:
        df=df0.loc[(df0.PRICE!=0)&(df0.NO<=time),:].copy()
    df['s1']=np.where((df.ACTION==1)&(df.BUYSELL=="S"), df.VOLUME,0)
    df['s2']=np.where((df.ACTION==2)&(df.BUYSELL=="S"), df.VOLUME,0)
    df['s0']=np.where((df.ACTION==0)&(df.BUYSELL=="S"), df.VOLUME,0)
    df['shid']=np.where((df.ACTION==1)&(df.BUYSELL=="S"), df.hid,0)
    df['b1']=np.where((df.ACTION==1)&(df.BUYSELL=="B"), df.VOLUME,0)
    df['b2']=np.where((df.ACTION==2)&(df.BUYSELL=="B"), df.VOLUME,0)
    df['b0']=np.where((df.ACTION==0)&(df.BUYSELL=="B"), df.VOLUME,0)
    df['bhid']=np.where((df.ACTION==1)&(df.BUYSELL=="B"), df.hid,0)
    df=df[['PRICE','ORDERNO','s1','s2','s0','shid','b1','b2','b0','bhid']]
    df=df.groupby(['PRICE','ORDERNO']).aggregate(np.sum)

    df['s_hiden']=np.where((df.s1==0)|(df.shid==0)|(df.shid-df.s2-df.s0<0),
                           0,
                           np.where(df.s2<df.s1, df.shid,np.nan))
    df['b_hiden']=np.where((df.b1==0)|(df.bhid==0)|(df.bhid-df.b2-df.b0<0),
                           0,
                           np.where(df.b2<df.b1, df.bhid,np.nan))

    df['buy']=df.b1+df.bhid-df.b2-df.b0
    df['sell']=df.s1+df.shid-df.s2-df.s0
    df['nbuy']=np.where(df.buy.values>0,1,0)
    df['nsell']=np.where(df.sell.values>0,1,0)
    df=df.reset_index(level=["PRICE","ORDERNO"])
    sord=df.loc[df.s_hiden.isnull(),].ORDERNO.values
    bord=df.loc[df.b_hiden.isnull(),].ORDERNO.values
    if sord.size>0:
        for order in sord:
            if NO==0:                
                a2=df0.loc[(df0.ACTION==2)&(df0.TIMEMLS<=time)&(df0.ORDERNO==order),:].VOLUME.values
            else:                
                a2=df0.loc[(df0.ACTION==2)&(df0.NO<=time)&(df0.ORDERNO==order),:].VOLUME.values
            a1=df.loc[df.ORDERNO==order,:].s1.values[0]
            h1=df.loc[df.ORDERNO==order,:].shid.values[0]
            if (a2[-1]>=a1)|(np.sum(a2)==a1):
                df.loc[df.ORDERNO==order,"s_hiden"]=h1-np.sum(a2)
            else:
                cs1=np.cumsum(a2[::-1])
                cs1=cs1[cs1<a1]
                cs1=cs1[-1]
                df.loc[df.ORDERNO==order,"s_hiden"]=h1-np.sum(a2)+cs1
    if bord.size>0:
        for order in bord:
            if NO==0:                
                a2=df0.loc[(df0.ACTION==2)&(df0.TIMEMLS<=time)&(df0.ORDERNO==order),:].VOLUME.values
            else:                
                a2=df0.loc[(df0.ACTION==2)&(df0.NO<=time)&(df0.ORDERNO==order),:].VOLUME.values
            a1=df.loc[df.ORDERNO==order,:].b1.values[0]
            h1=df.loc[df.ORDERNO==order,:].bhid.values[0]
            if (a2[-1]>=a1)|(np.sum(a2)==a1):
                df.loc[df.ORDERNO==order,"b_hiden"]=h1-np.sum(a2)
            else:
                cs1=np.cumsum(a2[::-1])
                cs1=cs1[cs1<a1]
                cs1=cs1[-1]
                df.loc[df.ORDERNO==order,"b_hiden"]=h1-np.sum(a2)+cs1
    df=(df[['PRICE','buy','sell','b_hiden','s_hiden','nbuy','nsell']]
        .groupby(['PRICE']).aggregate(np.sum)
        .reset_index(level=["PRICE"])
        .sort_values('PRICE', ascending=False)
        .query("buy!=0|sell!=0"))
    return df.iloc[(np.sum(df.buy==0)-10):(np.sum(df.buy==0)+10),:]