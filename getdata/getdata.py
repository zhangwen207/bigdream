#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import BDini,b2   #参数文件

from sqlalchemy import create_engine,text
from sqlalchemy.sql import select
import pymysql
import tushare as ts
from multiprocessing import Pool

engine = create_engine(BDini.DataBase,echo=True)

def getdata(code,LASTDATE,rowcount,num):
    """  
    获取历史前复权数据,返回近一年的复权数据
    date : 交易日期 (index)
    •open : 开盘价
    •high : 最高价
    •close : 收盘价
    •low : 最低价
    •volume : 成交量
    •amount : 成交金额
    """    
    b2.log('%s......进度......%d/%d'%(code,num,rowcount))
    print('%s......进度......%d/%d'%(code,num,rowcount))
    sqlcmd='select date_add(max(date) , interval 1 day) from '+'qfq'+code
        
    try:
        maxdate=engine.execute(sqlcmd)
        sdate=list(maxdate)[0][0].strftime('%Y-%m-%d')
        #print(sdate+'==='+LASTDATE)
        if sdate>LASTDATE:
            engine.execute(text('delete from GetBasics where code =:tb'),{'tb':code})
            b2.log('qfq%s......Skip'%code)
            return 
    except:
        try:
            ds=ts.get_h_data(code)
        except:
            b2.log('ts.get_h_data   qfq%s......ERROR'%code)
            return 
    else:
        try:
            ds=ts.get_h_data(code,start=sdate)
        except:
            b2.log('ts.get_h_data start=%s  qfq%s......ERROR'%(sdate,code))
            return  
        
    try:
        if ds.index[0].strftime('%Y-%m-%d')==LASTDATE:
            ds.to_sql('qfq'+code,engine,if_exists='append')  
            engine.execute(text('delete from GetBasics where code =:tb'),{'tb':code})
            b2.log('to_sql qfq%s......Append'%code)
            try:
                sqlcmd='alter table qfq%s drop index ix_qfq%s_date'%(code,code)
                engine.execute(sqlcmd)  
            except:
                b2.log(sqlcmd)       
            
            try:
                sqlcmd='create unique index  id_date on qfq%s (date)'%code
                engine.execute(sqlcmd)  
            except:
                b2.log(sqlcmd)                 
          
        #else:
            ##停牌股票不处理
            #engine.execute(text('delete from GetBasics where code =:tb'),{'tb':code})
            #b2.log('停牌股票不处理 delete from GetBasics where code =%s......ERROR'%code)
    except:
        b2.log('to_sql qfq'+code+'......ERROR')
        #停牌股票不处理
        #engine.execute(text('delete from GetBasics where code =:tb'),{'tb':code})  
        #b2.log('errcode delete from GetBasics where code =%s......ERROR'%code)
        
   
    return 





""" 获取沪深上市公司基本情况。属性包括：
code,代码
name,名称
industry,所属行业
area,地区
pe,市盈率
outstanding,流通股本
totals,总股本(万)
totalAssets,总资产(万)
liquidAssets,流动资产
fixedAssets,固定资产
reserved,公积金
reservedPerShare,每股公积金
eps,每股收益
bvps,每股净资
pb,市净率
timeToMarket,上市日期
"""

b2.log('获取沪深上市公司基本情况..........')
try:
    ds=ts.get_stock_basics()
except:
    print('ERROR: ts.get_stock_basics()')
else:
    #engine.execute('CREATE INDEX ix_basics_code ON basics (code(10))');
    engine.execute('truncate table basics');
    ds.to_sql('basics',engine,if_exists='append')
    
    engine.execute('truncate table GetBasics');
    ds.to_sql('GetBasics',engine,if_exists='append')    



"""
获取风险警示板股票数据，即查找所有st股票

参数说明：
•file_path:文件路径，默认为None即由TuShare提供，可以设定自己的股票文件路径。

返回值说明：
•code：股票代码
•name：股票名称

"""
b2.log('获取风险警示板股票数据..........')
try:
    ds=ts.get_st_classified()
except:
    print('ERROR: ts.get_st_classified()')
else:
    ds.to_sql('STcode',engine,if_exists='replace')


"""
成长能力

按年度、季度获取成长能力数据，结果返回的数据属性说明如下：


code,代码
name,名称
mbrg,主营业务收入增长率(%)
nprg,净利润增长率(%)
nav,净资产增长率
targ,总资产增长率
epsg,每股收益增长率
seg,股东权益增长率

"""

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
                    ds.to_sql('Growth',engine,if_exists='replace')
                except:
                    print('ERROR: ds.to_sql(%d,%d)'%(year,season))
                    continue
                else:
                    raise FoundException()
except FoundException:    pass

#读入数据    
b2.log('获取各股票行情数据..........')
di=ts.get_h_data('159915',index=True,start='2016-04-01')
LASTDATE=di.index[0].strftime('%Y-%m-%d')


#删除ST股票，删除主营业务收入增长低于40%的股票
engine.execute(text('delete from GetBasics  where code not in (select code from Growth where mbrg>40) or code in (select code from STcode)'))

for i in range(3):
    result=engine.execute(text('select * from GetBasics'))
    m=0
    p = Pool(processes=5)
    for row in result:
        m=m+1
        p.apply_async(getdata,[row[0],LASTDATE,result.rowcount,m])
    p.close()
    p.join()