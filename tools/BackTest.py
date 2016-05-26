#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import BDSys as BD
import BDBox as BOX
import matplotlib as mpl
import pandas as pd
from dateutil.parser import parse


def bt_init(xlst,prjNam,money0=1000000):
    """
    int,str,float -->qx 资产数据集合
    """
    #初始化资产数据集合
    qx=BD.zwQuantX(prjNam,money0)
    
    #从MYSQL读入资产价格数据到内存
    BOX.stkLibRd(xlst); 
    
    #  读取股票池数据,取最大的时间跨度
    xtim0=parse('9999-01-01');xtim9=parse('1000-01-01');
    for xcod in xlst:
        xt0=BD.stkLib[xcod].index[0]
        xt9=BD.stkLib[xcod].index[-1]
        if xtim0>xt0:xtim0=xt0
        if xtim9<xt9:xtim9=xt9  
        
    xtim0=xtim0.strftime('%Y-%m-%d');xtim9=xtim9.strftime('%Y-%m-%d')
    qx.qxTimSet(xtim0,xtim9)
    print(xtim0,xtim9,'\nstkCode',BD.stkLibCode)     
        


    
