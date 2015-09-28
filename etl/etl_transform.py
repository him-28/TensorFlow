#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: jyb
'''
import petl as etl
from petl import fromcsv
import datetime
import csv
import gc
import types

from time import sleep

class ETL_Transform:
    count=1
    def __init__(self,
                 header,
                 aggre_header,
                 csv_filePath,
                 batch_read_size=20000,
                 pdate=None,
                 hour=None):
        self.header=[i.strip() for i in header.strip().split(',')]
        self.aggre_header=[i.strip() for i in aggre_header.strip().split(',')]
        self.csv_filePath=csv_filePath
        self.batch_read_size=batch_read_size
        self.read_buffer=[]
        aggre_header_=self.aggre_header[:]
        aggre_header_.append('value')
        self.merge_table=[]
        self.merge_table.append(aggre_header_)
        self.pdate=pdate
        self.hour=hour
        
    def transform_supply(self):
        with open(self.csv_filePath,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                row=[i.strip() for i in line.strip().split('\t')]
                self.read_in_buffer(row)
                
        self.etl_aggregate_supply()
        self.read_buffer = []
        self.merge_table = etl.addfield(self.merge_table,'hit_total',lambda rec:rec['value'])
        self.merge_table = etl.addfield(self.merge_table,'date_id',self.pdate)
        self.merge_table = etl.addfield(self.merge_table,'time_id',self.hour)
        
        #format date
        d=datetime.datetime.strptime(self.pdate,"%Y-%m-%d")
        pdate_=datetime.datetime.strftime(d,"%Y_%m_%d")
        etl.tocsv(self.merge_table,"{0}_{1}_supply_pv_display.csv".format(pdate_,self.hour),encoding="utf-8",write_header=False) 
    def etl_aggregate_supply(self):
        row_table=[]
        row_table.append(self.header)
        row_table.extend(self.read_buffer)
         
        agg_header_ = self.aggre_header[:]
        
        display_table = etl.aggregate(row_table,tuple(self.aggre_header),len)
        agg_header_.append('value')
        try:
            tmp_table = self.merge_table.list()[:]
        except:
            tmp_table = self.merge_table[:]
        tmp_merge_table = etl.merge(tmp_table,display_table,key=tuple(agg_header_))
#         tmp_2_merge_table=etl.convert(tmp_merge_table,"value",lambda x:int(x))
        table_t = etl.aggregate(tmp_merge_table,tuple(self.aggre_header),sum,'value')
        self.merge_table=table_t
        
    def read_in_buffer(self,row):
        if len(self.read_buffer) <self.batch_read_size:
            self.read_buffer.append(row)
        else:
            self.read_buffer.append(row)
            self.etl_aggregate_supply()
            print "read:",self.count*self.batch_read_size
            self.count =self.count + 1
            self.read_buffer=[]
        
if __name__ == "__main__":
    etls=ETL_Transform(
                       "'boardid','deviceid','videoid','slotid','cardid','creativeid','p_v_hid','p_v_rid','p_v_rname','p_c_type',\
                       'p_c_ip','cityid','intime','p_c_idfa1','p_c_imei','p_c_ctmid','p_c_mac','p_c_anid','p_c_openudid','p_c_idfa',\
                       'p_c_odin','p_c_aaid','p_c_duid','sid'"
                       ,"'slotid','cardid','creativeid'",
                       csv_filePath=r"C:\Users\Administrator\Desktop\20150920.04.product.supply.csv",
                       batch_read_size=50000,
                       pdate="2015-09-09",
                       hour="11")
    
    etls.transform_supply()
