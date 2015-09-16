# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
#    +Author wennu
#    +Email wenu@e.hunantv.com


"""
load data from mongodb to local *.csv 
"""
import sys
import petl as etl
import csv
from pymongo import MongoClient
import pymongo
import pymysql
    
def load(day, hour):
    client = MongoClient('10.100.2.77', 27017)
    db = client.emap
    boardids = ['4389', '4480', '4540']
    for bid in boardids:
	collection = 'supply_' + bid + '_' + day + '_' + hour
	db_supply = db[collection]
	table = db_supply.find_one()
	#print collection.find_one()
	print table
	
        #conn = pymysql.connect(host='192.168.8.187', port=3306, user='root',passwd='root', db='test', charset='UTF8')
	#conn.cursor().execute('SET SQL_MODE=ANSI_QUOTES')
        #etl.todb(table, conn, 'etlload')
        #conn.commit()
        #conn.close()
    
if __name__ == "__main__":
    day = sys.argv[1]
    hour = sys.argv[2]
    
    load(day, hour)
    import  logging
    import traceback
    try:
        load(day,hour)
    except Exception,e:
        logging.error(e.args)
        ex = traceback.format_exc()
        #print 'this is e.printstack',ex
