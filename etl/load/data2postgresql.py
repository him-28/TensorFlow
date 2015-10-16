#-*- coding: utf-8 -*-
'''
Created on 2015年9月21日

@author: jyb
'''
import petl
import psycopg2
import datetime


class Pg_Loader():
    """
        把文件中的data Load到postgresql中.
    """
    conn=psycopg2.connect(database="postgres", user="postgres", password="", host="10.100.5.80", port=11921)
    def __init__(self, db_name="adc", 
                 db_table="AD_Facts_By_Hour",
                 insert_cols = "date_id,time_id,area_id,video_id,os_id,reqs_total,impressions_start_total,impressions_finish_total,click,hit_total,ad_slot_id,ad_card_id,ad_creative_id,ad_campaign_id",
                 primay_cols="date_id,time_id,area_id,video_id,os_id,ad_slot_id,ad_card_id,ad_creative_id,ad_campaign_id",
                 clear_cols="date_id,time_id",
                 db_host="10.100.5.80",
                 db_port="11921",
                 file_data_path=None,
                 batch_rows_size=200,
                 load_type="update"
                 ):
        
        self.file_data_path = file_data_path
        self.primay_cols = [i.strip() for i in primay_cols.split(',')]
        self.primay_cols = all(self.primay_cols) and self.primay_cols or []
        self.db_name = db_name
        self.db_table = db_table
        self.db_host = db_host
        self.insert_cols = [i.strip() for i in insert_cols.split(',')]
        self.insert_buff = []
        self.batch_rows_size=batch_rows_size
#         self.mysql_db_engine = zpool[self.mysql_dburl]
        self.value_cols = list(set(self.insert_cols) - set(self.primay_cols))
        self.insert_rows_count = 0
        self.insert_rows_count_tmp = 0
        self.load_type=load_type
        
    def load_file_to_pgtable(self):
        with open(self.file_data_path) as fr:
            for line in fr:
                row = line.strip().split('\t')
                if len(row) != len(self.insert_cols):
                    print "not correct row:",row,line
                    continue
                self.batch_insert_to_pg(row)
            #TODO insert
            self.insert_buff
    
    def load_list_to_pgtable(self,data_list):
        pass
    
    
    def batch_insert_to_pg(self,row):
        if len(self.insert_buff) < self.batch_rows_size:
            self.insert_buff.append(row)
        else:
            # TODO insert
            self.insert_buff=[]
    def insert_to_pg(self,buffer,retrytimes=3):
        for i in range(retrytimes):
            try:
                insert_rows(buffer)
                return True
            except Exception,e:
                print"[ERROR]", e
                pass
        print "RETRY {0} TIMES FAILED".format(retrytimes)
    
        
    def insert_rows(self,rows):
        if load_type and load_type == "etl":
            load_by_etl(rows)
        else:
            load_by_sql(rows)
            
            
    def load_by_sql(self,rows):
        if not rows or not len(rows):
                return None
        sql = "INSERT INTO {0}({1}) VALUES "
        for row in rows:
            col_val="("
            for val in row:
                if not val or val is None:
                    val='null'
                val_=val.strip()
                try:
                    int(val_)
                    col_val +="{0},".format(val_)
                except Exception,e:
                    col_val +="'{0}',".format(val_)
            col_val.rstrip(',')
            col_val +="),"
            sql += col_val
        sql.rstrip(',')
        sql=sql.format(self.db_table,self.insert_cols)
        #insert to db
        self.insert_excute_by_sql(sql)
        
            
    def load_by_etl(self,rows):
        if not self.insert_cols or not len(self.insert_cols):
            return None
        db_tab_vals=[]
        tab_col_name=[i.strip() for i in self.insert_cols.split(',')]
        if not tab_col_name:
            return None
        db_tab_vals.append(tab_col_name)
        if not rows or not len(rows):
            return None
        for row in rows:
            db_tab_vals.append(row)
        petl.appenddb(db_tab_vals,conn,self.db_table)
    
    def excute_by_sql(self,str_sql):
        try:
            cur = self.conn.cursor()
            if not str_sql:
                return 
            result=cur.execute(str_sql)
            self.conn.commit()
        except Exception,e:
            print e
        finally:
            if cur:
                cur.close()
            if self.conn:
                self.conn.close()
                
    def clear_db(self,conditions):
        if not conditions or not len(conditions):
            return None
        
        
                
if __name__ == "__main__":
#     str1 ="date_id,time_id,area_id,video_id,os_id,ad_slot_id,ad_card_id,ad_creative_id,ad_campaign_id"
#     str1 = [i.strip() for i in str1.split(',')]
#     print set(str1)
#     print list(set(str2)-set(str1))
#     loader=Pg_Loader()
#     loader.insert_excute_by_sql("insert into te(m) values (89),(45)")
    date_e=datetime.datetime.strptime("20150809"+"02","%Y%m%d%H")
    s=date_e.strftime("%Y-%m-%d")
    
    print date_e.strftime("%H")
