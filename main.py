#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import BDini,b2   #参数文件

import tushare as ts
from sqlalchemy import create_engine

engine = create_engine(BDini.DataBase,echo=True)


def initDB(StCode,LD='',Index=False):
    """
    StCode 证券代码，LD  最后交易日，Index   指数 --> LASTDATE 最后交易日
    检查最新数据，如果mysql DB没有，就下载，并写库。
    """
    if Index :      
        di=ts.get_h_data(StCode,index=Index)
        LASTDATE=di.index[0].strftime('%Y-%m-%d')
    else:
        pass
    
    return LASTDATE

def MBRG():
    b2.log('获取成长能力数据..........')
    class FoundException(Exception): pass
    
    try:
        for year in (2017,2016):
            for season in (4,3,2,1):
                try:
                    ds=ts.get_growth_data(year,season)
                except:
                    print('ERROR: ts.get_growth_data(%d,%d)'%(year,season))
                else:
                    try:
                        ds.to_sql('growth',engine,if_exists='replace')
                    except:
                        print('ERROR: ds.to_sql(%d,%d)'%(year,season))
                        continue
                    else:
                        raise FoundException()
    except FoundException:    pass   
    
    b2.log('获取风险警示板股票数据..........')
    ds=ts.get_st_classified()
    ds.to_sql('stcode',engine,if_exists='replace')
    
    #删除ST股票，删除主营业务收入增长低于40%的股票
    result=engine.execute('select * from growth where mbrg>40 and code not in (select code from stcode)')
    print(result)
    
    
    



def main():
    #策略名称
    CLName='MBRG-RSI-VAR9-20151027' 
    
    #最后交易日
    LDate=initDB('159915',LD='',Index=True)
    #print(LDate)
    
    #以下是策略
    #
    #策略1：基本面筛选  主营收益增长大于40%
    MBRG()
    


if __name__=='__main__':
    main()













"""





import BDTest as BT    #Back Test 回溯测试
import BDSys as BD    #数据结构
import BDsta  #数据准备
import BDta  #指标算法
import BDini   #参数文件

from dateutil import rrule
from sqlalchemy import create_engine
import numpy as np
import pandas as pd



def dataPre(qx,xnam0):  
    engine = create_engine(BDini.DataBase,echo=True)
    #剪裁数据，排除不再分析范围的数据
    BDsta.sta_dataPre0xtim(qx,xnam0)
    for xcod in BD.stkLibCode:
        df=BD.stkLib[xcod]
        #  计算交易价格kprice和策略分析采用的价格dprice,kprice一般采用次日的开盘价
        #买入价钱：明日以当日收盘价挂买单。如果买点当日收盘价>=明日最低价，则以当日收盘价为买入价，
        #否则，买入价为0，表示没买上              
        df['dprice']=df['close']
        df['kprice']=np.where(df['close']>=df['low'].shift(-1),df['close'],0)
        
        #
        d=qx.staVars[0];df=BDta.MA(df,d,'close');
        d=10;df=BDta.MA(df,d,'close');
        d=qx.staVars[1];df=BDta.MA(df,d,'close');
        #
        df=BDta.MACD(df)
        df=BDta.SAR(df)
        
        
        """
"""
VAR2赋值:1日前的最低价
VAR3赋值:最低价-VAR2的绝对值的3日[1日权重]移动平均/最低价-VAR2和0的较大值的3日[1日权重]移动平均*100
VAR4赋值:如果收盘价*1.3,返回VAR3*10,否则返回VAR3/10的3日指数移动平均
VAR5赋值:30日内最低价的最低值
VAR6赋值:30日内VAR4的最高值
VAR7赋值:如果收盘价的58日简单移动平均,返回1,否则返回0
VAR8赋值:如果最低价<=VAR5,返回(VAR4+VAR6*2)/2,否则返回0的3日指数移动平均/618*VAR7
VAR9赋值:如果VAR8>100,返回100,否则返回VAR8
MID赋值:收盘价的20日简单移动平均
VART1赋值:(收盘价-MID)的2乘幂
VART2赋值:VART1的20日简单移动平均
VART3赋值:VART2的开方
UPPER赋值:MID+2*VART3
LOWER赋值:MID-2*VART3
输出UB:1日前的UPPER,线宽为2,画黄色
输出LB:1日前的LOWER,线宽为2,画黄色
ZWSHORT1赋值:(1日前的收盘价>1日前的收盘价的10日简单移动平均*1.1 AND 最高价<1日前的最高价)
当满足条件ZWSHORT1时,在最高价*1.01位置画2号图标
ZWSHORT2赋值:(收盘价<(26日内收盘价的最高值*0.85) AND 平滑异同平均的DIF>0)
ZCOUNT2赋值:统计上次抛物转向.抛物转向<收盘价距今天数日中满足ZWSHORT2的天数
当满足条件ZCOUNT2=1ANDZWSHORT2时,在最高价*1.02位置画35号图标
RSI赋值:收盘价-1日前的收盘价和0的较大值的4日[1日权重]移动平均/收盘价-1日前的收盘价的绝对值的4日[1日权重]移动平均*100
CRSI赋值:(RSI上穿11) OR (RSI上穿20) 
N赋值:上次平滑异同平均的DIF>平滑异同平均的DEA距今天数和上次抛物转向.抛物转向<收盘价距今天数的较大值
ZCOUNT3赋值:统计N日中满足CRSI的天数
当满足条件ZCOUNT3=2ANDCRSIANDMACD.DIF<0时,在最低价*0.95位置画1号图标
当满足条件ZCOUNT3>=3ANDCRSIANDMACD.DIF<0时,在最低价*0.95位置画38号图标
CVAR赋值:(VAR9>2 AND VAR9<1日前的VAR9 AND 2日前的VAR9<=1日前的VAR9 )
ZCOUNT4赋值:统计N日中满足CVAR的天数
当满足条件ZCOUNT4=2ANDCVAR时,在最低价*0.95位置画34号图标
当满足条件ZCOUNT4=3ANDCVAR时,在最低价*0.95位置画25号图标
当满足条件ZCOUNT4>=4ANDCVAR时,在最低价*0.95位置画25号图标
当满足条件ZCOUNT4>=4ANDCVAR时,在最低价*0.90位置画25号图标




VAR2:=REF(LOW,1);
VAR3:=SMA(ABS(LOW-VAR2),3,1)/SMA(MAX(LOW-VAR2,0),3,1)*100;
VAR4:=EMA(IF(CLOSE*1.3,VAR3*10,VAR3/10),3);
VAR5:=LLV(LOW,30);
VAR6:=HHV(VAR4,30);
VAR7:=IF(MA(CLOSE,58),1,0);
VAR8:=EMA(IF(LOW<=VAR5,(VAR4+VAR6*2)/2,0),3)/618*VAR7;
VAR9:=IF(VAR8>100,100,VAR8);


MID:=MA(CLOSE,20);
VART1:=POW((CLOSE-MID),2);
VART2:=MA(VART1,20);
VART3:=SQRT(VART2);
UPPER:=MID+2*VART3;
LOWER:=MID-2*VART3;
UB:REF(UPPER,1),LINETHICK2,COLORYELLOW;
LB:REF(LOWER,1),LINETHICK2,COLORYELLOW;

{卖出：超越上轨10%，无法创新高}
ZWSHORT1:=(REF(CLOSE,1)>REF(MA(CLOSE,10)*1.1,1) AND HIGH<REF(HIGH,1));
DRAWICON(ZWSHORT1,HIGH*1.01,2);


{卖出：MACD.DEA零轴之上时，下跌超15%,大势已去，不宜久留，当天出现当天要走}
ZWSHORT2:=(CLOSE<(HHV(CLOSE,26)*0.85) AND MACD.DIF>0);
ZCOUNT2:=COUNT(ZWSHORT2,BARSLAST(SAR.SAR<CLOSE));
DRAWICON(ZCOUNT2=1 AND ZWSHORT2,HIGH*1.02,35);


{买入：一个下跌波段里，发生RSI上穿11或20，2次（含）以上}
RSI:=SMA(MAX(CLOSE-REF(CLOSE,1),0),4,1)/SMA(ABS(CLOSE-REF(CLOSE,1)),4,1)*100;
CRSI:=(CROSS(RSI,11)) OR (CROSS(RSI,20)) ;
{CRSI:=((FILTER(CROSS(RSI,11),7)) OR (FILTER(CROSS(RSI,20),7))) ;}
N:=MAX(BARSLAST(MACD.DIF>MACD.DEA),BARSLAST(SAR.SAR<CLOSE));
ZCOUNT3:=COUNT(CRSI,N);
DRAWICON(ZCOUNT3=2 AND CRSI AND MACD.DIF<0 ,LOW*0.95,1);
DRAWICON(ZCOUNT3>=3 AND CRSI AND MACD.DIF<0 ,LOW*0.95,38);

{买入：一个下跌波段里，出现2次（含）以上VAR9尖峰}
CVAR:=(VAR9>2 AND VAR9<REF(VAR9,1) AND REF(VAR9,2)<=REF(VAR9,1) );
ZCOUNT4:=COUNT(CVAR,N);
DRAWICON(ZCOUNT4=2 AND CVAR ,LOW*0.95,34);
DRAWICON(ZCOUNT4=3 AND CVAR ,LOW*0.95,25);
DRAWICON(ZCOUNT4>=4 AND CVAR ,LOW*0.95,25);
DRAWICON(ZCOUNT4>=4 AND CVAR ,LOW*0.90,25);


        #df['var2']=df['low'].shift(1)
        #df['var3']=ta.SMA(np.array(np.fabs(df['low']-df['var2'])),3)*100/ta.SMA(np.array(np.fmax(df['low']-df['var2'],0)),3)
        #df['var3']=df['var3'].replace(np.inf,df['var3'][np.isfinite(df['var3'])].max())
        #df['var4']=pd.ewma(np.array(df['var3']*10),span=3)
        ##df['var5']=pd.rolling_min(df['low'],30,1)
        #df['var5']=df['low'].rolling(min_periods=1,center=False,window=30).min()
        
        #df['var4']=df['var4'].replace(np.nan,0)
        ##df['var6']=pd.rolling_max(df['var4'],30,1)
        #df['var6']=df['var4'].rolling(min_periods=1,center=False,window=30).max()
        
        #df['var7']=df['close']/df['close']
        
        #df['var8']=pd.ewma(np.where(df['low']<=df['var5'],(df['var4']+df['var6']*2)/2,0),span=3)/618
        #df['var9']=np.where(df['var8']>=df['var8'].max(),df['var8'],0)
        #df.var9[df.var9>100]=100        
        
        ####计算卖出信号
        #{卖出：超越上轨10%，无法创新高}
        df['sell']=( df['high']<df['high'].shift(1) ) * (df['close'].shift(1) > df['ma_10'].shift(1)*1.1)
        #{卖出：MACD.DEA零轴之上时，下跌超15%,大势已去，不宜久留，当天出现当天要走}
        df['count']=BDta.COUNT((df['close']<0.85*df['close'].rolling(min_periods=1,center=False,window=26).max())*(df['MACDdiff']>0), BDta.BARSLAST(df['sar']>df['close']))
        df['count'][df['count']!=1]=0        
        df['sell']=df['sell']+df['count']        
        #print(df[df['BL']!=0])
        #*(df['sar']>df['close'])

        ####计算买入信号    
            
            
        BD.stkLib[xcod]=df
        print(df.tail())    
        #---
        fss='tmp_'+qx.prjName+'_'+xcod
        #engine.execute('truncate table %s'%fss)
        df.to_sql(fss,engine,if_exists='replace')        
     
def handle_data(qx,xnam0):
    '''
        tim0Trad(qx):
        零点交易策略，可看做是基于时间的：事件模式·回溯测试
        【输入】
            qx.stkCode，当前交易的股票代码
            qx.xtim，当前交易的时间
        【输出】
            srkNum，当前股票代码xcod，交易的股票数目：>0，买入；<0,卖出;=0,不交易
    '''     
    pass

########## main ###########

#评估资产集合
xlst=['000099','000096']

#资金量
zjl=1000000

#项目名称
StaName='bigdream01'


###########################

#生成一个资产数据集qx
qx=BT.bt_init(xlst,StaName,zjl);

#---设置策略参数 [5日均线,30日均线,起始日'yyyy-mm-dd',结束日'yyyy-mm-dd']   
qx.staVars=[5,30,'','']   

#---根据当前策略，对数据进行预处理
dataPre(qx,CLName)

#---绑定策略函数&运行回溯主函数
qx.staFun=handle_data(qx,CLName);
#BT.zwBackTest(qx)

"""