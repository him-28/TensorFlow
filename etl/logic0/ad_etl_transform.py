#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: jyb
'''
import petl as etl
import datetime
import sys
import os
# from etl.util.data2postgresql import load
from etl.conf.settings import APConfig as Config
from etl.conf.settings import LOGGER

class ETL_Transform:
    
    def __init__(self,header,
                 filePath,output_dic,batch_read_size=5000):
        self.header=header
        self.filePath=filePath
        self.batch_read_size=batch_read_size
        self.chance_select=Config["chance_select"]
        self.count_select=Config["count_select"]
        self.start_select=Config["start_select"]
        self.end_select=Config["end_select"]
        self.click_select=Config["click_select"]
        self.chance_aggre_header=Config["chance_aggre_header"]
        self.count_aggre_header=Config["count_aggre_header"]
        self.start_aggre_header=Config["start_aggre_header"]
        self.end_aggre_header=Config["end_aggre_header"]
        self.click_aggre_header=Config["click_aggre_header"]
        self.chance_merge_table=[]
        self.count_merge_table=[]
        self.start_merge_table=[]
        self.end_merge_table=[]
        self.click_merge_table=[]
        self.chance_agg_header_=self.chance_aggre_header[:]
        self.chance_agg_header_.append('value')
        self.count_agg_header_=self.count_aggre_header[:]
        self.count_agg_header_.append('value')
        self.start_agg_header_=self.start_aggre_header[:]
        self.start_agg_header_.append('value')
        self.end_agg_header_=self.end_aggre_header[:]
        self.end_agg_header_.append('value')
        self.click_agg_header_=self.click_aggre_header[:]
        self.click_agg_header_.append('value')
        self.chance_merge_table.append(self.chance_agg_header_)
        self.count_merge_table.append(self.count_agg_header_)
        self.start_merge_table.append(self.start_agg_header_)
        self.end_merge_table.append(self.end_agg_header_)
        self.click_merge_table.append(self.click_agg_header_)
        self.read_buffer=[]
        self.chance_buffer=[]
        self.count_buffer=[]
        self.imps_start_buffer=[]
        self.imps_end_buffer=[]
        self.click_buffer=[]
        self.output_dic=output_dic
        self.threads=[]
        
    def transform_start(self):
        LOGGER.info("etl...")
        self.read_buffer=[]
        with open(self.filePath,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                row=[i.strip() for i in line.strip().split('\t')]
                self.read_in_buffer(row)
                
        print "read over ..."
        self.read_buffer_in_table()
        self.read_buffer=[]
        self.chance_merge_table = etl.rename(self.chance_merge_table,'value','total')
        self.count_merge_table = etl.rename(self.count_merge_table,'value','total')
        self.start_merge_table = etl.rename(self.start_merge_table,'value','total')
        self.end_merge_table = etl.rename(self.end_merge_table,'value','total')
        self.click_merge_table = etl.rename(self.click_merge_table,'value','total')
        
    def etl_display_chance(self):
        self.chance_merge_table = self.etl_aggregate(self.chance_buffer,self.chance_aggre_header,self.chance_merge_table)
    def etl_display_count(self):
        self.count_merge_table = self.etl_aggregate(self.count_buffer,self.count_aggre_header,self.count_merge_table)
    def etl_imps_start(self):
        self.start_merge_table = self.etl_aggregate(self.imps_start_buffer,self.start_aggre_header,self.start_merge_table)
    def etl_imps_end(self):
        self.end_merge_table = self.etl_aggregate(self.imps_end_buffer,self.end_aggre_header,self.end_merge_table)
    def etl_click(self):
        self.click_merge_table = self.etl_aggregate(self.click_buffer,self.click_aggre_header,self.click_merge_table)
        
    def etl_aggregate(self,t_buffer,aggre_header,merge_table):
#         row_table=[]
#         row_table.append(self.header)
#         row_table.extend(t_buffer)
        row_table=t_buffer
        agg_header_ = aggre_header[:]
        
        tuple_key=tuple(aggre_header)
        if len(aggre_header) == 1:
            tuple_key=aggre_header[0]
        display_table = etl.aggregate(row_table,tuple_key,len)
        agg_header_.append('value')
        tmp_merge_table=self.union_table(merge_table, display_table)
#         tmp_2_merge_table=etl.convert(tmp_merge_table,"value",lambda x:int(x))
        table_t = etl.aggregate(tmp_merge_table,tuple_key,sum,'value')
        return table_t
        
    def read_in_buffer(self,row):
        if len(self.read_buffer) <self.batch_read_size:
            self.read_buffer.append(row)
        else:
            self.read_buffer.append(row)
            self.read_buffer_in_table()
            self.read_buffer=[]
    def read_buffer_in_table(self):
        self.chance_buffer=self.select_data(self.read_buffer, self.chance_select)
        self.etl_display_chance()
        self.chance_buffer=None
        self.count_buffer=self.select_data(self.read_buffer, self.count_select)
        self.etl_display_count()
        self.count_buffer=None
        self.imps_start_buffer=self.select_data(self.read_buffer, self.start_select)
        self.etl_imps_start()
        self.imps_start_buffer=None
        self.imps_end_buffer=self.select_data(self.read_buffer, self.end_select)
        self.etl_imps_end()
        self.imps_end_buffer=None
        self.click_buffer=self.select_data(self.read_buffer, self.click_select)
        self.etl_click()
        self.click_buffer=None
        
        for t in self.threads:
            print t
            t.start()
        for t in self.threads:
            t.join()
            
    def select_data(self,rows,q_select):
        tmp_table=[]
        tmp_table.append(self.header)
        tmp_table.extend(rows)
        t_=etl.select(tmp_table,q_select)
        return t_
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
        self.transform_start()
        # table ad_facts_by_hour
#         import time
#         time.sleep(2)
        if not os.path.exists(self.filePath):
            os.makedirs(self.filePath)
            
        chance_file=self.output_dic.get(Config["display_pos"])
        count_file=self.output_dic.get(Config["display"])
        start_file=self.output_dic.get(Config["impression"])
        end_file=self.output_dic.get(Config["impression_end"])
        click_file=self.output_dic.get(Config["click"])
            
        LOGGER.info("generate "+chance_file)
        etl.tocsv(self.chance_merge_table,chance_file,encoding="utf-8",write_header=True)
        
        LOGGER.info("generate "+count_file)
        etl.tocsv(self.count_merge_table,count_file,encoding="utf-8",write_header=True)
        
        LOGGER.info("generate "+start_file)
        etl.tocsv(self.start_merge_table,start_file,encoding="utf-8",write_header=True)
        
        LOGGER.info("generate "+end_file)
        etl.tocsv(self.end_merge_table,end_file,encoding="utf-8",write_header=True)
        
        LOGGER.info("generate "+click_file)
        etl.tocsv(self.click_merge_table,click_file,encoding="utf-8",write_header=True)
        
        
def calc_etl(input_filePath,input_filename,output_dic):
    #check  param
    assert input_filePath,input_filename is not None
    
    filePath = os.path.join(input_filePath,input_filename)
    etls=ETL_Transform(
                       header=Config['header'],
                       batch_read_size=Config["batch_read_size"],
                       filePath=filePath,output_dic=output_dic)
    try:
        LOGGER.info("petl etl start...")
        etls.transform()
    except Exception,e:
        import traceback
        ex=traceback.format_exc()
        LOGGER.error(ex)
        sys.exit(-1)

if __name__ == "__main__":
    day = sys.argv[1]
    hour = sys.argv[2]
#     hour_etl(day, hour)
    calc_etl("C:\Users\Administrator\Desktop\\" ,"ad(1).csv",
             {'display_pos':'C:\Users\Administrator\Desktop\\201510\chance.csv',
              'display':'C:\Users\Administrator\Desktop\\201510\count.csv',
              'impression':'C:\Users\Administrator\Desktop\\201510\start.csv',
              'impression_end':'C:\Users\Administrator\Desktop\\201510\end.csv',
              'click':'C:\Users\Administrator\Desktop\\201510\click.csv'})

    #ip    ty    ur    v    ci    b    rs    l    td    req    s    cp    c    ct    o    e    pf    d    u    net    os    mf    mod    app    ts    si
