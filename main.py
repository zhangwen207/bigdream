#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import BDini,b2   #参数文件
import BDta  #指标算法

import tushare as ts
from sqlalchemy import create_engine
from multiprocessing import Pool
import pandas.io.sql as sql
import numpy as np
import talib as ta
import pandas as pd
from pandas import Series,DataFrame

engine = create_engine(BDini.DataBase,echo=True)

def readydata(tbname):
    """
    检查mysql数据和交易日数据是否一致，是否需要下载
    tbname 表名  -->
    返回值
    0 没下载或数据相差一天，需要下载  
    1 数据一致，不需要下载  
    -1 当日已下载，但相差不止一天，估计是停牌，忽略
    """
    sqlcmd='''select count(*) from tb_stamp where TableName='%s' and TStamp>=(select max(date) from zs159915)'''%tbname
    result=engine.execute(sqlcmd)
    if list(result)[0][0]==0:
        ret=0
    elif tbname[:3]=='qfq':
        try:
            sqlcmd='''SELECT CASE WHEN (select date_add(max(date) , interval 1 day) from  %s )=(select max(date) from zs159915) THEN 0 WHEN (select max(date) from  %s )=(select max(date) from zs159915) THEN 1 ELSE -1 END'''%(tbname,tbname)
            result=engine.execute(sqlcmd)
            ret=list(result)[0][0]
        except:
            ret=0
    else:
        ret=1
    return ret


def ggzbtj(CLName,code,rowcount,num):
    """
    个股指标统计
    CLName 策略名称,code  股票代码,LASTDATE 最后交易日,rowcount 总股票数,num 第几个 -->无
    
    """
    b2.log('%s......进度......%d/%d'%(code,num,rowcount))
    print('%s......进度......%d/%d'%(code,num,rowcount))
    
    #检查数据下载情况
    df=initData(code)

    if df is None:
        return
    
    b2.log('计算%s卖买指标........'%code)
    print('计算%s卖买指标..........'%code)    
    """
    以下是指标计算

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
    
    
    {卖出：超越上轨10%，无法创新高}
    ZWSHORT1:=(REF(CLOSE,1)>REF(MA(CLOSE,10)*1.1,1) AND HIGH<REF(HIGH,1));
    DRAWICON(ZWSHORT1,HIGH*1.01,2);
    
    
    {卖出：MACD.DEA零轴之上时，下跌超15%,大势已去，不宜久留，当天出现当天要走}
    ZWSHORT2:=(CLOSE<(HHV(CLOSE,26)*0.85) AND MACD.DIF>0);
    ZCOUNT2:=COUNT(ZWSHORT2,BARSLAST(SAR.SAR<CLOSE));
    DRAWICON(ZCOUNT2=1 AND ZWSHORT2,HIGH*1.02,35);
    
    

    """
    #  计算交易价格kprice和策略分析采用的价格dprice,kprice一般采用次日的开盘价
    #买入价钱：明日以当日收盘价挂买单。如果买点当日收盘价>=明日最低价，则以当日收盘价为买入价，
    #否则，买入价为0，表示没买上         
    #df['dprice']=df['close']
    df['kprice']=np.where(df['close']>=df['low'].shift(-1),df['close'],0)   
    
    df=BDta.MA(df,5,'close');
    df=BDta.MA(df,10,'close');
    #
    df=BDta.MACD(df)
    df=BDta.SAR(df)
    
    
    ####计算卖出信号
    #{卖出：超越上轨10%，无法创新高}
    df['sell']=( df['high']<df['high'].shift(1) ) & (df['close'].shift(1) > df['ma_10'].shift(1)*1.1)

    #{卖出：MACD.DEA零轴之上时，下跌超15%,大势已去，不宜久留，当天出现当天要走}
    df['count']=BDta.COUNT((df['close']<0.85*df['close'].rolling(min_periods=1,center=False,window=26).max()) & (df['MACDdiff']>0), BDta.BARSLAST(df['sar']>df['close']))


    #整理卖出信号，sell=1
    #连续发出卖出信号，则标记首次
    df['count'][df['count']!=1]=0
    #汇总两个卖出信号
    df['sell']=df['sell']+df['count']  



    ####计算买入信号 
    #{买入：一个下跌波段里，发生RSI上穿11或20，2次（含）以上}
    df=BDta.RSI(df)
    df['crsi']=BDta.CROSS(df['rsi'],11) 
    df['crsi2']=BDta.CROSS(df['rsi'],20)
    df['crsi'][df['crsi2']==1]=1 
    df['N0']=BDta.BARSLAST(df['sar']>df['close'])
    df['N1']=BDta.BARSLAST(df['MACDdiff']<df['MACDdea'])

    df['N']=np.fmax(df.N1, df.N0)
    df['bcount']=BDta.COUNT(df['crsi'], df['N'])
    df['bcount'][df['bcount']<2]=0
    df['bcount'][df['bcount']!=0]=df['bcount']-1

    #{买入：一个下跌波段里，出现2次（含）以上VAR9尖峰}
    df['var2']=df['low'].shift(1)
    df['var3']=ta.SMA(np.array(np.fabs(df['low']-df['var2'])),3)*100/ta.SMA(np.array(np.fmax(df['low']-df['var2'],0)),3)
    
    df['var3']=df['var3'].replace(np.inf,df['var3'][np.isfinite(df['var3'])].max())
    df['var4']=pd.ewma(np.array(df['var3']*10),span=3)
    df['var5']=df['low'].rolling(min_periods=1,center=False,window=30).min()

    df['var4']=df['var4'].replace(np.nan,0)
    df['var6']=df['var4'].rolling(min_periods=1,center=False,window=30).max()

    #df['var7']=df['close']/df['close']

    df['var8']=pd.ewma(np.where(df['low']<=df['var5'],(df['var4']+df['var6']*2)/2,0),span=3)/618

    df['CVAR']=df['var8']<df['var8'].shift(1)
    df['CVAR'][df['var8'].shift(2)>df['var8'].shift(1)]=0

    df['ccount']=BDta.COUNT(df['CVAR'], df['N'])

    df['ccount'][df['ccount']!=0]=df['ccount']-1

    df['buy']=df['bcount']+df['ccount']
    df['profit']=0.0
    
    #卖出策略
    

    buyprice=0
    cs=0  #次数
    xsum=0
    for i in df.index:
        ii=i
        s=df.ix[i]
        buy=s.buy  #买入标志
        sell=s.sell  #卖出标志
        kprice=s.kprice #卖出价格        

        #buy=df.get_value(i,'buy')  #买入标志
        #sell=df.get_value(i,'sell')  #卖出标志
        #kprice=df.get_value(i,'kprice') #卖出价格
          
        
        
        if buyprice==0 and s.buy ==0 :
            continue
        
        if s.sell==1 and buyprice !=0:
            df.set_value(i,'profit',(s.kprice*cs-xsum)*100/buyprice)
            buyprice=0
            cs=0    
            xsum=0
            continue
        
        if s.buy != 0 and s.kprice !=0:
            cs=cs+1
            buyprice=s.kprice
            xsum=xsum+buyprice
            continue
        
        if buyprice!=0 and s.buy ==0 and s.sell ==0:
            if s.high>=buyprice*1.05  :
                df.set_value(i,'profit',(buyprice*1.05*cs-xsum)*100/buyprice)
                df.set_value(i,'sell',1)
                buyprice=0
                cs=0 
                xsum=0
            if s.low<=buyprice*0.85:
                df.set_value(i,'profit',(buyprice*0.85*cs-xsum)*100/buyprice)
                df.set_value(i,'sell',1)
                buyprice=0
                cs=0 
                xsum=0
            continue
                
                
    
    #持仓价值，取最后一日收盘价
    if buyprice !=0:
        df.set_value(ii,'profit',(df.get_value(ii,'close')*cs-xsum)*100/buyprice)
        df.set_value(ii,'sell',1)
        
    mrnum=df['buy'][(df.buy>0) & (df.kprice >0)].count()
    engine.execute('''insert into score values (curdate(),'%s','%s',%d,%d,%f,%f,%d,%d)'''%(code,CLName,mrnum,df['sell'][df.sell>0].count(),df['profit'].sum(),df['profit'][df.profit>0].count()*100/mrnum,df.get_value(ii,'buy'),df.get_value(ii,'sell')))
        
    print(df.loc[:,('buy','sell','kprice','profit')][df['buy']!=0|df['sell']])
    df.to_sql('zb'+code,engine,if_exists='replace')         
    
    #sqlcmd='select * from score order by ratio'
    #ds=sql.read_sql(sqlcmd,engine)    
    #print(ds)

    
   
     


        

def bxfx(CLName,codes):
    """
    并行分析
    CLName 策略标识，codes  股票集合 -->无
    
    """
    #最后交易日
    ds=initData('159915',Index=True)
    engine.execute('delete from  score where date=curdate()')
    
    #测试
    #engine.execute('delete from  score where date=curdate() and code=002723' )    
    
    #print(ds)
    #print(LDate)
    
    m=0
    p = Pool(processes=5)
    for row in codes:
        m=m+1
        p.apply_async(ggzbtj,[CLName,row[0],codes.rowcount,m])
    p.close()
    p.join()
    
    
def initData(code,Index=False):
    """
    code 证券代码，LD  最后交易日，Index   指数 --> ds 数据集，LASTDATE 最后交易日
    检查最新数据，如果mysql DB没有，就下载，并写库。
    如果不是没有最新交易日数据（例如：没数据或停牌），返回None
    """
    b2.log('获取%s数据..........'%code)
    print('获取%s数据..........'%code)    
    if Index :      
        ds=ts.get_h_data(code,index=Index)
        #LASTDATE=ds.index[0].strftime('%Y-%m-%d')
        ds.to_sql('zs'+code,engine,if_exists='replace') 
    else:
        tbstatus=readydata('qfq%s'%code)
        if  tbstatus==1:
            b2.log('qfq%s......Data Ready'%code)
            sqlcmd='select * from qfq%s order by date'%code
            ds=sql.read_sql(sqlcmd,engine,index_col='date')                
        elif tbstatus==0:
            ds=ts.get_h_data(code).sort()
            ds.to_sql('qfq'+code,engine,if_exists='replace') 
            engine.execute('''insert into tb_stamp values ('qfq%s',curdate())'''%code)
        else:
            ds=None
    return ds
        
        

def MBRG():
    """
    无 --> result 符合条件的股票集合
    
    算法：排除ST股票以及主营业务收入增长低于40%的股票
    """    
    
    b2.log('获取主营业务收入增长数据..........')
    print('获取主营业务收入增长数据..........')
    #print(readydata('growth')[0])
    if readydata('growth')==0:
    #季报数据不一定及时，因此采用试错办法
        class FoundException(Exception): pass       
        try:
            for year in (2016,):
                for season in (4,3,2,1):
                    try:
                        ds=ts.get_growth_data(year,season)
                    except:
                        print('ERROR: ts.get_growth_data(%d,%d)'%(year,season))
                    else:
                        try:
                            ds.to_sql('growth',engine,if_exists='replace')
                            engine.execute('''insert into tb_stamp values ('growth',curdate())''')
                        except:
                            print('ERROR: ds.to_sql(%d,%d)'%(year,season))
                            continue
                        else:
                            raise FoundException()
        except FoundException:    pass   
    
    b2.log('获取风险警示板股票数据..........')
    if not readydata('stcode'):
        ds=ts.get_st_classified()
        ds.to_sql('stcode',engine,if_exists='replace')
        engine.execute('''insert into tb_stamp values ('stcode',curdate())''')
    
    #删除ST股票，删除主营业务收入增长低于40%的股票
    result=engine.execute('select distinct code from growth where mbrg>0 and code not in (select code from stcode)')
    
    #测试测试
    #result=engine.execute('select distinct code from growth where code=002723')

    return result
    
    
    



def main():
    #策略名称
    CLName='MBRG-RSI-VAR9-20151027' 
    

    
    #以下是策略
    #
    #策略1：基本面筛选  主营收益增长大于40%
    MBRG_codes=MBRG()
    
    #策略2：个股并行分析
    bxfx(CLName,MBRG_codes)
    


if __name__=='__main__':
    main()




