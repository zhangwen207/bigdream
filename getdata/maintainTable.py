#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import BDini

from sqlalchemy import create_engine,text
from sqlalchemy.sql import select
import pymysql


engine = create_engine(BDini.DataBase,echo=True)



#sqlcmd='''select table_name from information_schema.tables where table_name like  'qfq______' ''' + ' or table_name like '+''' 'tmp%' '''
sqlcmd='''select table_name from information_schema.tables where table_name like  'qfq______' or table_name like  'tmp_bigdream01_______'  '''
result=engine.execute(sqlcmd)
for rec in result:
    try:
        sqlcmd='drop table %s'%rec[0]
        maxdate=engine.execute(sqlcmd)
    except:
        print('drop table......error')
    




    

