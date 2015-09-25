#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: jyb
'''
import petl as etl
import csv
import gc

table=[['m'],[1],[2]]
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
        self.aggre_header=[i.strip() for i in header.strip().split(',')]
        self.header=['boardid','deviceid','videoid','slotid','cardid','creativeid','p_v_hid','p_v_rid','p_v_rname','p_c_type',
                       'p_c_ip','cityid','intime','p_c_idfa1','p_c_imei','p_c_ctmid','p_c_mac','p_c_anid','p_c_openudid','p_c_idfa',
                       'p_c_odin','p_c_aaid','p_c_duid','sid']
        self.aggre_header=['slotid','cardid','creativeid']
        self.csv_filePath=csv_filePath
        self.batch_read_size=batch_read_size
        self.read_buffer=[]
        aggre_header_=self.aggre_header[:]
        aggre_header_.append('value')
        self.merge_table=[]
        self.merge_table.append(aggre_header_)
        self.pdate=pdate
        self.hour=hour
        
    def transform(self):
#         etl.aggregate(table1, 'cardid', len)
#         header = ['slotid', 'cardid', 'creativeid', 'deviceid', 'type', 'intime', 'ip', 'boardid', '_sid', 'videoid', 'second']
#         #{boardid}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}
        self.header=['boardid','deviceid','videoid','slotid','cardid','creativeid','p_v_hid','p_v_rid','p_v_rname','p_c_type',
                       'p_c_ip','cityid','intime','p_c_idfa1','p_c_imei','p_c_ctmid','p_c_mac','p_c_anid','p_c_openudid','p_c_idfa',
                       'p_c_odin','p_c_aaid','p_c_duid','sid']
        self.aggre_header=['slotid','cardid','creativeid']
#         
#         #聚合的列 supply  pv 展示 一次算5000条（可配置）
#         # 聚合结果
#         etl.aggregate()
#         table1 = etl.split(table,'67\t990\t669\tmgtvmac1048B14F6247\t1\t1442952002\t175.150.183.141\t4820\tnull\t1739456\t0', '\t', header)
#         table2 = etl.convert(table1, 'intime', lambda v: datetime.datetime.fromtimestamp(int(v)).strftime('%Y-%m-%d %H:%M:%S'))
#         etl.addfield(table2, 'hour', lambda rec: datetime.datetime.strptime(rec['intime'], '%Y-%m-%d %H:%M:%S').hour)
#         
#         if not self.header or not self.csv_filePath:
#             return None
        
        with open(self.csv_filePath,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                row=[i.strip() for i in line.strip().split('\t')]
                self.read_in_buffer(row)
                
        self.etl_aggregate()
        self.read_buffer=[]
        self.merge_table
#             etl.tocsv(self.merge_table,"ssss.csv",encoding="utf-8",write_header=False) 
    def etl_aggregate(self):
#         table1=[['a','b','c','d'],['a1','b1','2','2'],['a2','b2','3','4']]
#         table1=[['a','b','c','d']]
#         table2=[['a','b','c','d'],['a1','b1','4','5'],['a3','b3','5','6']]
#         print table1
#         table3=etl.merge(table1,table2,key=('a','b','c','d'))
#         print table3
#         table4=etl.convert(table3,('c','d'),lambda x:int(x))
#         print table4
#         table5=etl.aggregate(table4,('a','b'),sum,'c')
#         table6=etl.aggregate(table4,('a','b'),sum,'d')
#         print table5
#         print table6
#         table7=etl.join(table5,table6,key=('a','b'))
#         print table7
#         etl.tocsv(table7,"tttt.csv",encoding="utf-8",write_header=False) 
        
        row_table=[]
        row_table.append(self.header)
        for row in self.read_buffer:
            row_table.append(row)
         
        agg_header_ = self.aggre_header[:]
        
        display_table = etl.aggregate(row_table,tuple(self.aggre_header),len)
#         row_table=None
        del row_table
        agg_header_.append('value')
        tmp_merge_table = etl.merge(self.merge_table,display_table,key=tuple(agg_header_))
#         display_table=None
        del display_table
        self.merge_table = etl.aggregate(tmp_merge_table,tuple(self.aggre_header),sum,'value')
#         tmp_merge_table=None
        del tmp_merge_table
        self.count +=1
        print "down over",self.count
        gc.collect()
        
    def read_in_buffer(self,row):
        if len(self.read_buffer) <self.batch_read_size:
            self.read_buffer.append(row)
        else:
            self.read_buffer.append(row)
            self.etl_aggregate()
            self.read_buffer=[]
        
if __name__ == "__main__":
    import time
    etls=ETL_Transform("'aa','aad'","", r"C:\Users\Administrator\Desktop\20150923.10.product.supply.csv",batch_read_size=50000)
    
    print time.localtime()
    etls.transform()
    print time.localtime()
    
#     etls.batch_read_size=15000
#     etls.transform("", "")
#     print time.localtime()
#     
#     etls.batch_read_size=20000
#     etls.transform("", "")
#     print time.localtime()