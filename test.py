#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import BDini,b2   #参数文件
import BDta  #指标算法

import tushare as ts
from sqlalchemy import create_engine
from multiprocessing import Pool
import pandas.io.sql as sql
import numpy as np

engine = create_engine(BDini.DataBase,echo=True)

def readydata(tbname):
    sqlcmd='''select count(*) from tb_stamp where TableName='%s' and TStamp=curdate()'''%tbname
    result=engine.execute(sqlcmd)
    return result.first()


def ggzbtj(CLName,code,LASTDATE,rowcount,num):
    """
    个股指标统计
    CLName 策略名称,code  股票代码,LASTDATE 最后交易日,rowcount 总股票数,num 第几个 -->无
    
    """
    b2.log('%s......进度......%d/%d'%(code,num,rowcount))
    print('%s......进度......%d/%d'%(code,num,rowcount))
    
    #检查数据下载情况
    df,LD=initData(code,LASTDATE)

    if LD is None:
        return
    
    """
    以下是指标计算
    """
    #  计算交易价格kprice和策略分析采用的价格dprice,kprice一般采用次日的开盘价
    #买入价钱：明日以当日收盘价挂买单。如果买点当日收盘价>=明日最低价，则以当日收盘价为买入价，
    #否则，买入价为0，表示没买上         
    df['dprice']=df['close']
    df['kprice']=np.where(df['close']>=df['low'].shift(-1),df['close'],0)    
    print(df)
    
   
     


        

def bxfx(CLName,codes):
    """
    并行分析
    CLName 策略标识，codes  股票集合 -->无
    
    """
    #最后交易日
    ds,LDate=initData('159915',LD='',Index=True)
    
    m=0
    p = Pool(processes=5)
    for row in codes:
        m=m+1
        p.apply_async(ggzbtj,[CLName,row[0],LDate,codes.rowcount,m])
    p.close()
    p.join()
    
    
def initData(code,LD='',Index=False):
    """
    code 证券代码，LD  最后交易日，Index   指数 --> ds 数据集，LASTDATE 最后交易日
    检查最新数据，如果mysql DB没有，就下载，并写库。
    如果不是没有最新交易日数据（例如：没数据或停牌），返回None
    """
    b2.log('获取%s数据..........'%code)
    if Index :      
        ds=ts.get_h_data(code,index=Index)
        LASTDATE=ds.index[0].strftime('%Y-%m-%d')
        return ds,LASTDATE
    else:
        sqlcmd='select date_add(max(date) , interval 1 day) from '+'qfq'+code
        try:
            maxdate=engine.execute(sqlcmd)
            sdate=list(maxdate)[0][0].strftime('%Y-%m-%d')
            #print(sdate+'==='+LASTDATE)
            if sdate>LD:
                b2.log('qfq%s......Data Ready'%code)
                sqlcmd='select * from qfq%s order by date'%code
                ds=sql.read_sql(sqlcmd,engine,index_col='date')                
                return ds,LD
        except:
            b2.log('Warning:qfq%s......New Data Table'%code)

        ds=ts.get_h_data(code)
        ds.to_sql('qfq'+code,engine,if_exists='replace') 
        maxdate=engine.execute(sqlcmd)
        sdate=list(maxdate)[0][0].strftime('%Y-%m-%d')
        #print(sdate+'==='+LASTDATE)
        if sdate>LD:
            b2.log('qfq%s......Data Ready'%code)
            return ds,LD        
        else:
            return None,None
        
        

def MBRG():
    """
    无 --> result 符合条件的股票集合
    
    算法：排除ST股票以及主营业务收入增长低于40%的股票
    """    
    
    b2.log('获取主营业务收入增长数据..........')
    print('获取主营业务收入增长数据..........')
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
    ds=ts.get_st_classified()
    ds.to_sql('stcode',engine,if_exists='replace')
    engine.execute('''insert into tb_stamp values ('stcode',curdate())''')
    
    #删除ST股票，删除主营业务收入增长低于40%的股票
    #result=engine.execute('select distinct code from growth where mbrg>40 and code not in (select code from stcode)')
    
    #测试测试
    result=engine.execute('select distinct code from growth where code=603636')

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
#    main()
    print(readydata('growth')[0])













