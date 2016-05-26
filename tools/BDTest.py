#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import BDSys as BD
import BDBox as BOX
import matplotlib as mpl
import pandas as pd
from dateutil.parser import parse


def bt_init(xlst,prjNam,money0=1000000):
    """
    int,str,float -->qx 资产数据集合初始化
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
    return qx

def zwBackTest(qx,CLName):
    '''
    qx数据包，CLName策略名称       回溯测试主程序

    '''
    #计算回溯时间周期，也可以在此，根据nday调整回溯周期长度
    #或者在 qt_init数据初始化设置，通过qx.qxTimSet(xtim0,xtim9)，设置回溯周期长度
    nday=qx.periodNDay;print('nday',nday);   #nday=3;
    #按时间循环，进行回溯测试
    for tc in range(nday):
        tim5=qx.DTxtim0+dt.timedelta(days=tc) 
        xtim=tim5.strftime('%Y-%m-%d')  #        print('tc',tc,xtim)
        #每个测试时间点，开始时，清除qx相关参数
        qx.qxTim0SetVar(xtim);  #qx.prQxUsr();#qx.xtim=xtim;
        xpriceFlag=False;  #有效交易标志Flag
        #按设定的股票代码列表，循环进行回溯测试
        for xcod in BD.stkLibCode:
            #qx.stBDkCode=xcod;    #print('xcod',xcod)
            #xdatWrk是当前xcod，=stkLib[xcod]
            #xbarWrk是当前时间点的stkLib[xcod]
            #注意,已经包括了，qt_init里面的扩充数据列
            qx.xbarWrk,qx.xdatWrk=BOX.xbarGet8TimExt(xcod,qx.xtim);
            #print(xcod,'xbar\n',qx.xbarWrk)
            if not qx.xbarWrk[qx.priceWrk].empty:
                xpriceFlag=True
                # 调用回溯子程序，如果是有效交易，设置成功交易标志xtrdFlag
                zwBackTest100(qx)
            
        #如果所有股票代码列表循环完毕，成功交易标志为真
        #在当前测试时间点终止，设置有关交易参数
        if xpriceFlag:
            qx.wrkNDay+=1
            qx.qxTim9SetVar(qx.xtim);
            ###qx.xtim9Wrk=xtim;？？？

def zwBackTest100(qx):
    '''
    zwBackTest100(qx):
    zwQT回溯测试子函数，测试一只股票xcod，在指定时间xtim的回溯表现数据
    会调用qx.staFun指定的策略分析函数，获取当前的股票交易数目 qx.stkNum
    并且根据股票交易数目 qx.stkNum，判定是不是有效的交易策略
    【输入】
            qx.stkCode，当前交易的股票代码
            qx.xtim，当前交易的时间
    【输出】
         无
    '''
    
    #----运行策略函数，进行策略分析
    qx.stkNum=qx.staFun(qx);
    #----
    if qx.stkNum!=0:
        #----检查，是不是有效交易
        xfg,qx.xtrdChk=zwx.xtrdChkFlag(qx)
        if xfg:
            #----如果是有效交易，加入交易列表
            zwx.xtrdLibAdd(qx)
            #qx.prQCap();
            
        


    
