#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import BDini

from sqlalchemy import create_engine,text
from sqlalchemy.sql import select
import pymysql


engine = create_engine(BDini.DataBase,echo=True)



sqlcmd='''select table_name from information_schema.tables where table_name like 'qfq______' or table_name like 'tmp%'; '''
result=engine.execute(sqlcmd)
#for i in range(3):
    #errcode=[]
for rec in result:
    #sqlcmd='delete from %s  where date in (select date from %s group by date having count(date) > 1) and rowid not in (select min(rowid) from %s group by date having count(date)>1)'%(tablename,tablename,tablename)
    try:
        sqlcmd='drop table %s'%rec[0]
        maxdate=engine.execute(sqlcmd)
    except:
        print('drop table......error')
    




    

