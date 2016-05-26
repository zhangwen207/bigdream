#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from dateutil.parser import parse
from dateutil import rrule
import datetime as dt
import pandas as pd





stkLib={}       #全局变量，相关股票的交易数据，内存股票数据库
stkLibCode=[]   #全局变量，相关股票的交易代码，内存股票数据库
qxLibName=['date','stkVal','cash','dret','val','downLow','downHigh','downDay','downKMax'];
qxLibNil=['',0,0,0,0,0,0,0,0];  #xBars:DF

class zwQuantX(object):
    def __init__(self,prjNam,dbase0):
        self.prjName=prjNam;
        
        #起步资金
        self.mbase=dbase0;
        self.money=dbase0;        

        #------设置各种环境的价格模式：
        #    priceWrk，策略分析时，使用的股票价格，一般是：dprice，复权开盘价
        #    priceBuy，买入/卖出的股票价格，一般是：kprice，一般采用次日的复权开盘价
        #    priceCalc，最后结算使用的股票价格，一般是：adjclose，复权收盘价

        self.priceBuy='kdprice'
        self.priceCalc='adjclose'
        self.priceWrk='dprice' 
        
        xtim0='';xtim9=''  #起始日期，结束日期
        self.xtim0=xtim0
        self.xtim9=xtim9        
        self.DTxtim0=dt.datetime.today()
        self.DTxtim9=dt.datetime.today()
        
        self.rfRate=0         #计算sharp radio无风险利率
        
        self.staVars=[] #---设置策略参数 比如[5日均线,30日均线,起始日'yyyy-mm-dd',结束日'yyyy-mm-dd'] 
        self.staName=''  #项目名称
        
        self.qxUsr=pd.Series(index=qxLibName)
        
    def qxTimSet(self,xtim0,xtim9):
        #字符型日期变量
        self.xtim0=xtim0
        self.xtim9=xtim9
        self.DTxtim0=parse(self.xtim0)
        self.DTxtim9=parse(self.xtim9)
        self.periodNDay=rrule.rrule(rrule.DAILY,dtstart=self.DTxtim0,until=self.DTxtim9).count()       
        
    def qxTim0SetVar(self,xtim):
        self.xtim=xtim;
        self.qxUsr['date']=xtim;
        self.qxID=0;        
        
    