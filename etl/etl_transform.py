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
from data2postgresql import load
import yaml
Config=yaml.load(file("config.yml"))

class ETL_Transform:
    count=1
    def __init__(self,
                 supply_header,
                 aggre_header,
                 demand_header,supply_csv_filePath,demand_csv_filePath,batch_read_size=20000,
                 pdate=None,hour=None,version=None,type_t=None):
        self.supply_header= supply_header
        self.aggre_header=aggre_header
        self.demand_header=demand_header
        self.supply_csv_filePath=supply_csv_filePath
        self.demand_csv_filePath=demand_csv_filePath
        self.batch_read_size=batch_read_size
        self.read_buffer=[]
        aggre_header_=self.aggre_header[:]
        aggre_header_.append('value')
        self.supply_merge_table=[]
        self.demand_merge_table=[]
        self.supply_merge_table.append(aggre_header_)
        self.demand_merge_table.append(aggre_header_)
        self.demand_merge_table_click = []
        self.demand_merge_table_start = []
        self.demand_merge_table_end = []
        self.demand_merge_table_click.append(aggre_header_)
        self.demand_merge_table_start.append(aggre_header_)
        self.demand_merge_table_end.append(aggre_header_)
        self.pdate=pdate
        self.hour=hour
        self.version=version
        self.type_t=type_t
        
        
    def transform_supply(self):
        print "supply etl"
        self.read_buffer=[]
        with open(self.supply_csv_filePath,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                row=[i.strip() for i in line.strip().split('\t')]
                self.supply_read_in_buffer(row)
                
        self.etl_aggregate_supply()
        self.read_buffer = []
        self.supply_merge_table = etl.rename(self.supply_merge_table,'value','total')
        self.supply_merge_table = etl.addfield(self.supply_merge_table,'date_id',self.pdate)
        self.supply_merge_table = etl.addfield(self.supply_merge_table,'time_id',self.hour)
        
        #format date
        d=datetime.datetime.strptime(self.pdate,"%Y%m%d")
        pdate_=datetime.datetime.strftime(d,"%Y_%m_%d")
#         etl.tocsv(self.supply_merge_table,"{0}_{1}_supply_pv_display.csv".format(pdate_,self.hour),encoding="utf-8",write_header=False) 
        
    def etl_aggregate_supply(self):
        row_table=[]
        row_table.append(self.supply_header)
        row_table.extend(self.read_buffer)
         
        agg_header_ = self.aggre_header[:]
        
        display_table = etl.aggregate(row_table,tuple(self.aggre_header),len)
        agg_header_.append('value')
        try:
            tmp_table = self.supply_merge_table.list()
        except:
            tmp_table = self.supply_merge_table
#        tmp_merge_table = etl.merge(self.supply_merge_table,display_table,key=tuple(agg_header_))
        tmp_merge_table=self.union_table(self.supply_merge_table, display_table)
#         tmp_2_merge_table=etl.convert(tmp_merge_table,"value",lambda x:int(x))
        table_t = etl.aggregate(tmp_merge_table,tuple(self.aggre_header),sum,'value')
        self.supply_merge_table=table_t
        
    def transfrom_demand(self):
        print "demand etl"
        self.read_buffer=[]
        with open(self.demand_csv_filePath,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                row=[i.strip() for i in line.strip().split('\t')]
                self.demand_read_in_buffer(row)
                
        self.etl_aggregate_demand()
        self.read_buffer = []
        
        click_agg_table = etl.rename(self.demand_merge_table_click,'value','click')
        imp_start_agg_table = etl.rename(self.demand_merge_table_start,'value','impressions_start_total')
        imp_end_agg_table = etl.rename(self.demand_merge_table_end,'value','impressions_finish_total')
        
        demand_merge_table=etl.merge(click_agg_table,imp_start_agg_table,imp_end_agg_table,key=tuple(self.aggre_header))
        self.demand_merge_table=etl.convert(demand_merge_table,{'click':lambda x:(0 if not x else x),'impressions_start_total':lambda x:(0 if not x else x),'impressions_finish_total':lambda x:(0 if not x else x)})
#         eml2=etl.convert(eml,('click','d','e'),lambda x:int(x))
        
        self.demand_merge_table = etl.addfield(self.demand_merge_table,'date_id',self.pdate)
        self.demand_merge_table = etl.addfield(self.demand_merge_table,'time_id',self.hour)
        
        #format date
        d=datetime.datetime.strptime(self.pdate,"%Y%m%d")
        pdate_=datetime.datetime.strftime(d,"%Y_%m_%d")
#         etl.tocsv(self.demand_merge_table,"{0}_{1}_demand_click_imps_start_end.csv".format(pdate_,self.hour),encoding="utf-8",write_header=True)
        
    def etl_aggregate_demand(self):
        row_table=[]
        row_table.append(self.demand_header)
        row_table.extend(self.read_buffer)
         
        agg_header_ = self.aggre_header[:]
        agg_header_.append('value')
        # 1印象检测 2点击检测
        #second 0 开始   3600 结束
        #统计开始播放数，结束播放数，和点击数
        click_table = etl.select(row_table,"{type} == '2'")
        click_agg_table_t = etl.aggregate(click_table, tuple(self.aggre_header), len)
#         click_agg_table = etl.rename(click_agg_table_t,'value','click') 
#         try:
#             tmp_table = self.demand_merge_table_click.list()
#         except:
#             tmp_table = self.demand_merge_table_click
#         tmp_merge_table = etl.merge(tmp_table,click_agg_table_t,key=tuple(agg_header_))
        tmp_merge_table=self.union_table(self.demand_merge_table_click, click_agg_table_t)
        self.demand_merge_table_click = etl.aggregate(tmp_merge_table,tuple(self.aggre_header),sum,'value')
        
        imp_start_table = etl.select(row_table,"{type} == '1' and {second} == '0'")
        imp_start_agg_table_t = etl.aggregate(imp_start_table,tuple(self.aggre_header),len)
#         imp_start_agg_table = etl.rename(imp_start_agg_table_t,'value','impressions_start_total')
#         try:
#             tmp_table = self.demand_merge_table_start.list()
#         except:
#             tmp_table = self.demand_merge_table_start
#         tmp_merge_table = etl.merge(tmp_table,imp_start_agg_table_t,key=tuple(agg_header_))
        tmp_merge_table=self.union_table(self.demand_merge_table_start, imp_start_agg_table_t)
        self.demand_merge_table_start = etl.aggregate(tmp_merge_table,tuple(self.aggre_header),sum,'value')
        
        imp_end_table = etl.select(row_table,"{type} == '1' and {second} == '3600'")
        imp_end_agg_table_t = etl.aggregate(imp_end_table,tuple(self.aggre_header),len)
#         imp_end_agg_table = etl.rename(imp_end_agg_table_t,'value','impressions_finish_total')
#         try:
#             tmp_table = self.demand_merge_table_end.list()
#         except:
#             tmp_table = self.demand_merge_table_end
#         tmp_merge_table = etl.merge(tmp_table,imp_end_agg_table_t,key=tuple(agg_header_))
        tmp_merge_table=self.union_table(self.demand_merge_table_end, imp_end_agg_table_t)
        self.demand_merge_table_end = etl.aggregate(tmp_merge_table,tuple(self.aggre_header),sum,'value')
        
    def supply_read_in_buffer(self,row):
        if len(self.read_buffer) <self.batch_read_size:
            self.read_buffer.append(row)
        else:
            self.read_buffer.append(row)
            self.etl_aggregate_supply()
            print "read:",self.count*self.batch_read_size
            self.count =self.count + 1
            self.read_buffer=[]
    def demand_read_in_buffer(self,row):
        if len(self.read_buffer) <self.batch_read_size:
            self.read_buffer.append(row)
        else:
            self.read_buffer.append(row)
            self.etl_aggregate_demand()
            print "read:",self.count*self.batch_read_size
            self.count =self.count + 1
            self.read_buffer=[]
            
    def union_table(self,table1,table2):
        try:
            tmp_table = table1.list()
        except:
            tmp_table = table1
        try:
            tmp_table2 = table2.list()
        except:
            tmp_table2 = table2
        tmp_table.extend(tmp_table2[1:])
        return tmp_table
    def transform(self):
        self.transform_supply()
        self.transfrom_demand()
        if self.type_t == 'day' and self.version == 'old':
            facts_file_path=Config["old_version"]["day_hour_facts_file_path"]
        elif self.type_t == 'hour' and self.version == 'old':
            facts_file_path=Config["old_version"]["hour_facts_file_path"]
        elif self.type_t == 'day':
            facts_file_path=Config["day_hour_facts_file_path"]
        else:
            facts_file_path=Config["hour_facts_file_path"]
        # table ad_facts_by_hour
        hour_ad_fact_table = etl.convert(self.demand_merge_table,{'click':lambda x:(0 if not x else x),
                                                                'impressions_start_total':lambda x:(0 if not x else x),
                                                                'impressions_finish_total':lambda x:(0 if not x else x)
                                                                })
        d=datetime.datetime.strptime(self.pdate,"%Y%m%d")
        yearmonth=datetime.datetime.strftime(d,"%Y%m")
        pdate_=datetime.datetime.strftime(d,"%Y_%m_%d")
        if not os.path.exists(facts_file_path+"{0}".format(yearmonth)):
            os.makedirs(facts_file_path+"{0}".format(yearmonth))
        etl.tocsv(hour_ad_fact_table,facts_file_path+"{0}/{1}_{2}_ad_facts_by_hour.csv".format(yearmonth,pdate_,self.hour),encoding="utf-8",write_header=True)
        
        #table hit_facts_by_hour
        hout_hit_facts_table = etl.select(self.supply_merge_table,"{ad_card_id} != '-1' and {ad_creative_id} != '-1'")
        etl.tocsv(hout_hit_facts_table, facts_file_path+"{0}/{1}_{2}_hit_facts_by_hour.csv".format(yearmonth,pdate_,self.hour), encoding="utf-8",write_header=True)
        
        #table reqs_facts_by_hour
        aggre_tmp = self.aggre_header[:]
        aggre_tmp.remove("ad_card_id")
        aggre_tmp.remove("ad_creative_id")
        aggre_tmp.append("date_id")
        aggre_tmp.append("time_id")
        tmp_ = etl.aggregate(self.supply_merge_table,tuple(aggre_tmp),sum,'total')
        hour_reqs_facts_table = etl.rename(tmp_,'value','total')
        etl.tocsv(hour_reqs_facts_table, facts_file_path+"{0}/{1}_{2}_reqs_facts_by_hour.csv".format(yearmonth,pdate_,self.hour), encoding="utf-8",write_header=True)
        
    def load_to_pg(self):
        if self.type_t == 'hour':
            load('hour',self.pdate,Config["db_table"]["Ad_Facts_By_Hour"]["table_name"],self.version,self.hour)
            load('hour',self.pdate,Config["db_table"]["Hit_Facts_By_Hour"]["table_name"],self.version,self.hour)
            load('hour',self.pdate,Config["db_table"]["Reqs_Facts_By_Hour"]["table_name"],self.version,self.hour)
        
def hour_etl(day,hour,type_t,version):
    header_name=''
    if type_t == 'hour':
        header_name='agg_hour_header'
    elif type_t == 'day':
        header_name='agg_day_header'
    d = datetime.datetime.strptime(day,"%Y%m%d")
    yearmonth = datetime.datetime.strftime(d,"%Y%m")
    yearmonthday = datetime.datetime.strftime(d,"%Y%m%d")
    supply_filePath = Config["supply_csv_file_path"]+"{0}/{1}.{2}.product.supply.csv"
    demand_filePath = Config["demand_csv_file_path"]+"{0}/{1}.{2}.product.demand.csv"
     
    if version == 'old':
        supply_header=Config["old_version"]["supply"]["raw_header"]
        demand_header=Config["old_version"]["demand"]["raw_header"]
        aggre_header=Config["old_version"]["supply"][header_name]
    else:
        supply_header=Config["supply"]["raw_header"]
        demand_header=Config["demand"]["raw_header"]
        aggre_header=Config["supply"][header_name]
    etls=ETL_Transform(
                       supply_header=supply_header,
                       demand_header=demand_header,
                       aggre_header=aggre_header,
                       supply_csv_filePath=supply_filePath.format(yearmonth,yearmonthday,hour),
                       demand_csv_filePath=demand_filePath.format(yearmonth,yearmonthday,hour),
                       batch_read_size=Config["petl"]["batch_read_size"],
                       pdate=day,
                       hour=hour,
                       version=version,
                       type_t=type_t)
    try:
        etls.transform()
        etls.load_to_pg()
    except Exception,e:
        import traceback
        ex=traceback.format_exc()
        print ex
        print "ERROR:etl_transform",day,hour,e
if __name__ == "__main__":
    day = sys.argv[1]
    hour = sys.argv[2]
    type_t = sys.argv[3]
    version = sys.argv[4]
    hour_etl(day, hour, type_t, version)
