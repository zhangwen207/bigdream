#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import BDSys as BD
import BDini 
import pandas as pd

from sqlalchemy import create_engine
import pandas.io.sql as sql

def stkLibRd(xlst): 
    """
    list    读行情数据到全局变量BD.stkLib[]
    """
    engine = create_engine(BDini.DataBase,echo=True)
    BD.stkLibCode.extend(xlst);
    for xcod in xlst:
        sqlcmd='select * from qfq%s order by date'%xcod
        df=sql.read_sql(sqlcmd,engine,index_col='date')
        df['adjclose']=df['close']
        BD.stkLib[xcod]=df
        #BD.stkLib[xcod]=df.sort_index();
        #print(df)
        #d=BD.stkLib[xcod].index
        #print(d)
        #print(d[0])
        #print(d[-1])
        #print(BD.stkLib[xcod])
        
def stkLibSet8XTim(dtim0,dtim9):
    """
    str,str 删除不在范围内的数据
    """
    for xcod in BD.stkLibCode:
        df10=BD.stkLib[xcod]
        if dtim0=='':
            df20=df10;
        else:
            df20=df10[(df10.index>=dtim0)&(df10.index<=dtim9)]
        BD.stkLib[xcod]=df20.sort_index();
      

def qxObjSet(xtim,stkVal,dcash,dret):
    """
    str,float,float,float -->Series  日期，股票市值，现金，波动值
    """
    qx10=pd.Series(BD.qxLibNil,index=BD.qxLibName)
    qx10['date']=xtim;qx10['cash']=dcash
    qx10['stkVal']=stkVal
    qx10['val']=stkVal+dcash   
    return qx10

def xbarGet8TimExt(xcod,xtim):
    d10=BD.stkLib[xcod]
    d02=d10[xtim:xtim];
    
    return d02,d10