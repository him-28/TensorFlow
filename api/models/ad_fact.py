#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
获取数据连接，并从根据sql语句获取数据库数据
'''
import psycopg2

class dqc_dao():
    
    def select_all_by_date(self,date_range):
        conn = psycopg2.connect(database="postgres", user="postgres", password="", host="10.100.5.80", port="11921")
        cur = conn.cursor()
        #cur.execute("CREATE TABLE test(id serial PRIMARY KEY, num integer,data varchar);")
        # insert one item
        #cur.execute("INSERT INTO test(num, data)VALUES(%s, %s)", (1, 'aaa'))
        #cur.execute("INSERT INTO test(num, data)VALUES(%s, %s)", (2, 'bbb'))
        #cur.execute("INSERT INTO test(num, data)VALUES(%s, %s)", (3, 'ccc'))
        
        #解析date_range 
        if date_range.has_key("start"):
            start_date=date_range["start"]
        if date_range.has_key("end"):
            end_date=date_range["end"]
         
        cur.execute("SELECT * FROM test limit 10;")
        rows = cur.fetchall()        # all rows in table
        print(rows)
        for i in rows:
            print i
        
        conn.commit()
        cur.close()
        conn.close()
        return rows
        