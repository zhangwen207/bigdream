#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import talib as ta
import numpy as np

def MA(df, n,ksgn='close'):  
    '''
    def MA(df, n,ksgn='close'):  
    #Moving Average  
    MA是简单平均线，也就是平常说的均线
    【输入】
        df, pd.dataframe格式数据源
        n，时间长度
        ksgn，列名，一般是：close收盘价
    【输出】    
        df, pd.dataframe格式数据源,
        增加了一栏：ma_{n}，均线数据
    '''
    xnam='ma_{n}'.format(n=n)
    #ds5 = pd.Series(pd.rolling_mean(df[ksgn], n), name =xnam) 
    ds5 = pd.Series(df[ksgn].rolling(center=False,window=n).mean(), name =xnam) 
    df = df.join(ds5)  
    
    return df

#MACD, MACD Signal and MACD difference  
def MACD(df, n_fast=12, n_slow=26,ksgn='close'): 
    '''
    def MACD(df, n_fast, n_slow):           
      #MACD指标信号和MACD的区别, MACD Signal and MACD difference   
    MACD是查拉尔·阿佩尔(Geral Appel)于1979年提出的，由一快及一慢指数移动平均（EMA）之间的差计算出来。
    “快”指短时期的EMA，而“慢”则指长时期的EMA，最常用的是12及26日EMA：

    【输入】
        df, pd.dataframe格式数据源
        n，时间长度
        ksgn，列名，一般是：close收盘价
    【输出】    
        df, pd.dataframe格式数据源,
        增加了3栏：macd,sign,mdiff
    '''
    df['MACDdiff'] = ta.EMA(np.array(df['close']),n_fast)-ta.EMA(np.array(df['close']),n_slow)  
    df['MACDdea'] = ta.EMA(np.array(df['MACDdiff']),9)  
    df['MACD'] = 2*(df['MACDdiff'] - df['MACDdea'])    
    return df

#RSI  
def RSI(df): 
    '''
    增加一列sar

    '''
    df['rsi0']=ta.RSI(np.array(df['close']),timeperiod=6)  
    #df['ref1']=df['close'].shift(1)
    #df['rsi']=ta.SMA(np.array(np.fmax(df.close-df.ref1,0)),6)/ta.SMA(np.array(np.fabs(df.close-df.ref1)),6)*100
    return df

"""
6.上一个条件到当前的周期数
def BARSLAST(con):
参数输入：
    con：0/1 Series
返回：list
"""

def BARSLAST(con):
    ret=[]
    li=con.tolist() 
    for i in range(len(li)):
        m=0
        for j in range(i):
            if li[i-j]==0:
                break
            else:
                m=m+1
        ret.append(m)
    return ret

 
def SAR(df): 
    '''
    增加一列sar

    '''
    df['sar']=ta.SAR(np.array(df['high']),np.array(df['close']))   
    return df

def COUNT(con1,con2): 
    '''
    增加一列sar

    '''
    ret=[]
    li1=con1 
    li2=con2  
    for i in range(len(li2)):
        m=0
        for j in range(li2[i]):
                m=m+int(li1[i-j:i-j+1])
        if li1[i]:
            ret.append(m)    
        else:
            ret.append(0)
    return ret

def CROSS(li,num):
    ret=[0]
    if not isinstance(li,(int,float)):
        listli=li.tolist()  
        
    if isinstance(num,(int,float)):
        listnum= [ num for i in range(len(listli))]
    else:
        listnum=num.tolist() 
        
    if isinstance(li,(int,float)):
        listli= [ li for i in range(len(listlnum))] 
    

    for i in range(len(listli)-1):
        if listli[i+1]>=listnum[i+1] and listli[i]<listnum[i]:
            ret.append(1)
        else:
            ret.append(0)
    return ret
