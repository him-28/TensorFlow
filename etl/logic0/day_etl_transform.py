#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: jyb
'''
import petl as etl
from petl import fromcsv
import datetime
import csv
import types
import sys
import os
from etl.util.data2postgresql import load
from etl.util import init_log
from etl.conf.settings import Config
from etl.conf.settings import LOGGER
from etl_transform import ETL_Transform
from etl_transform import hour_etl

def etl_by_hour(day,hour,version):
    LOGGER.info("etl hour reload:"+day+"-"+hour)
    hour_etl(day,hour,'day',version)
    
def load_to_pg(day,version):
    LOGGER.info("load day file to db:"+day)
    load('day',day,Config["db_table"]["Ad_Facts_By_Day"]["table_name"],version,'')
    load('day',day,Config["db_table"]["Hit_Facts_By_Day"]["table_name"],version,'')
    load('day',day,Config["db_table"]["Reqs_Facts_By_Day"]["table_name"],version,'')
def day_etl_agg_hour(day,version):
    tmp_ad_facts_result_table=[]
    tmp_hit_facts_result_table=[]
    tmp_reqs_facts_result_table=[]
    d=datetime.datetime.strptime(day,"%Y%m%d")
    yearmonth=datetime.datetime.strftime(d,"%Y%m")
    pdate_=datetime.datetime.strftime(d,"%Y_%m_%d")
    if version == 'old':
        hour_facts_file_path=Config["old_version"]["day_hour_facts_file_path"]
        day_facts_file_path=Config["old_version"]["day_facts_file_path"]
    else:
        hour_facts_file_path=Config["hour_facts_file_path"]
        day_facts_file_path=Config["day_facts_file_path"]
    #合并24小时
    for hour in range(24):
        str_hour=''
        if hour < 10:
            str_hour='0'+str(hour)
        else:
            str_hour=str(hour)
            
        LOGGER.info("etl merge hour file:"+hour_facts_file_path+"{0}/{1}_{2}_ad_facts_by_hour.csv".format(yearmonth,pdate_,str_hour))
        hour_ad_facts_table = etl.fromcsv(hour_facts_file_path+"{0}/{1}_{2}_ad_facts_by_hour.csv".format(yearmonth,pdate_,str_hour),"utf-8")
        header=hour_ad_facts_table.list()[0]
        if tmp_ad_facts_result_table:
            tmp_ad_facts_result_table = etl.merge(tmp_ad_facts_result_table,hour_ad_facts_table,key=header)
        else:
            tmp_ad_facts_result_table=hour_ad_facts_table
        
        LOGGER.info("etl merge hour file:"+hour_facts_file_path+"{0}/{1}_{2}_hit_facts_by_hour.csv".format(yearmonth,pdate_,str_hour))
        hour_hit_facts_table = etl.fromcsv(hour_facts_file_path+"{0}/{1}_{2}_hit_facts_by_hour.csv".format(yearmonth,pdate_,str_hour),"utf-8")
        header=hour_hit_facts_table.list()[0]
        if tmp_hit_facts_result_table:
            tmp_hit_facts_result_table = etl.merge(tmp_hit_facts_result_table,hour_hit_facts_table,key=header)
        else:
            tmp_hit_facts_result_table=hour_hit_facts_table
        
        LOGGER.info("etl merge hour file:"+hour_facts_file_path+"{0}/{1}_{2}_reqs_facts_by_hour.csv".format(yearmonth,pdate_,str_hour))
        hour_reqs_facts_table = etl.fromcsv(hour_facts_file_path+"{0}/{1}_{2}_reqs_facts_by_hour.csv".format(yearmonth,pdate_,str_hour),"utf-8")
        header=hour_reqs_facts_table.list()[0]
        if tmp_reqs_facts_result_table:
            tmp_reqs_facts_result_table = etl.merge(tmp_reqs_facts_result_table,hour_reqs_facts_table,key=header)
        else:
            tmp_reqs_facts_result_table=hour_reqs_facts_table
    #聚合24小时数据
    day_hit_facts_table=aggre_hit_facts(tmp_hit_facts_result_table,version)
    day_reqs_facts_table=aggre_reqs_facts(tmp_reqs_facts_result_table,version)
    day_ad_facts_table=aggre_ad_facts(tmp_ad_facts_result_table,version)
    
    if not os.path.exists(day_facts_file_path+"{0}".format(yearmonth)):
            os.makedirs(day_facts_file_path+"{0}".format(yearmonth))
    LOGGER.info("generate day file:"+day_facts_file_path+"{0}/{1}_hit_facts_by_day.csv".format(yearmonth,pdate_))
    etl.tocsv(day_hit_facts_table, day_facts_file_path+"{0}/{1}_hit_facts_by_day.csv".format(yearmonth,pdate_), encoding="utf-8",write_header=True)
    
    LOGGER.info("generate day file:"+day_facts_file_path+"{0}/{1}_reqs_facts_by_day.csv".format(yearmonth,pdate_))
    etl.tocsv(day_reqs_facts_table, day_facts_file_path+"{0}/{1}_reqs_facts_by_day.csv".format(yearmonth,pdate_), encoding="utf-8",write_header=True)
    
    LOGGER.info("generate day file:"+day_facts_file_path+"{0}/{1}_ad_facts_by_day.csv".format(yearmonth,pdate_))
    etl.tocsv(day_ad_facts_table, day_facts_file_path+"{0}/{1}_ad_facts_by_day.csv".format(yearmonth,pdate_), encoding="utf-8",write_header=True)        
    
def aggre_hit_facts(table,version):
    if version == 'old':
        agg_header=Config["old_version"]["supply"]["agg_day_header"][:]
    else:
        agg_header=Config["supply"]["agg_day_header"][:]
    agg_header.append("date_id")
    table=etl.convert(table,'total',int)
    agg_table_ = etl.aggregate(table,tuple(agg_header),sum,'total')
    ren_table = etl.rename(agg_table_,'value','total')
    return ren_table
def aggre_reqs_facts(table,version):
    if version == 'old':
        agg_header=Config["old_version"]["supply"]["reqs_day_header"][:]
    else:
        agg_header=Config["supply"]["reqs_day_header"][:]
    agg_header.append("date_id")
    table=etl.convert(table,'total',int)
    agg_table_ = etl.aggregate(table,tuple(agg_header),sum,'total')
    ren_table = etl.rename(agg_table_,'value','total')
    return ren_table
def aggre_ad_facts(table,version):
    if version == 'old':
        agg_header=Config["old_version"]["supply"]["agg_day_header"][:]
    else:
        agg_header=Config["supply"]["agg_day_header"][:]
    agg_header.append("date_id")
    ad_header=agg_header[:]
    ad_header.append("impressions_start_total")
    ad_header.append("impressions_finish_total")
    ad_header.append("click")
    table=etl.convert(table,('impressions_start_total','impressions_finish_total','click'),int)
    agg_table_ = etl.rowreduce(table,key=tuple(agg_header),reducer=sum_ad_facts_reducer,header=ad_header)
    return agg_table_
def sum_ad_facts_reducer(key,rows):
    sum_imps_start=0
    sum_imps_end=0
    sum_click=0
    for row in rows:
        sum_imps_start +=row['impressions_start_total']
        sum_imps_end +=row['impressions_finish_total']
        sum_click +=row['click']
    result=list(key)
    result.append(sum_imps_start)
    result.append(sum_imps_end)
    result.append(sum_click)
    return result
def day_etl(day,type_t,version):
    if type_t == "reload":
        for hour in range(1,25):
            str_hour=''
            if hour < 10:
                str_hour='0'+str(hour)
            else:
                str_hour=str(hour)
            etl_by_hour(day, str_hour,version)
    elif type_t == 'merge':
        pass
    try:
        day_etl_agg_hour(day,version)
        load_to_pg(day,version)
    except Exception,e:
        import traceback
        ex=traceback.format_exc()
        LOGGER.error("day:"+day+" message:"+e.message)
        LOGGER.error(ex)
        sys.exit(-1)
if __name__ == "__main__":
    day=sys.argv[1]
    type_t=sys.argv[2] #reload merge
    version=sys.argv[3] #old new
    day_etl(day, type_t, version)
