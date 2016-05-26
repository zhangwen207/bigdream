#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import BDBox as BOX

def sta_dataPre0xtim(qx,xnam0):
    '''
    设置策略参数，根据预设时间，裁剪数据源stkLib
    '''
    #设置当前策略的变量参数
    qx.staName=xnam0
    qx.rfRate=0.05  #无风险年收益，一般为0.05(5%)，计算夏普指数等需要
    
    #按指定的时间周期，裁剪数据源
    xt0k=qx.staVars[-2];xt9k=qx.staVars[-1]
    if (xt0k!='')or(xt9k!=''):
        if xt0k!='':
            if qx.xtim0<xt0k:qx.xtim0=xt0k
        if xt9k!='':                
            if qx.xtim9>xt9k:qx.xtim9=xt9k
        qx.qxTimSet(qx.xtim0,qx.xtim9)
        
        #删除不在范围内的数据
        BOX.stkLibSet8XTim(qx.xtim0,qx.xtim9)  

    #---设置qxUsr用户数据
    qx.qxUsr=BOX.qxObjSet(qx.xtim0,0,qx.money,0);    
