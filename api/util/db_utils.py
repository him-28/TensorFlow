#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
'''
import psycopg2
import os
DIR_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
import sys
sys.path.append(DIR_PATH)
sys.path.append(os.path.join(DIR_PATH,'..'))
import settings
import traceback

def get_resultFromDb(str_sql):
    try:
        conn = psycopg2.connect(database=settings.db, user=settings.user, password=settings.password, host=settings.host, port=settings.db_port)
        cur = conn.cursor()
        if not str_sql:
            return 
        cur.execute(str_sql)
        rows = cur.fetchall()        # all rows in table
        conn.commit()
        cur.close()
        conn.close()
    except Exception,e:
        ex = traceback.format_exc()
        print ex
        return
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    return rows