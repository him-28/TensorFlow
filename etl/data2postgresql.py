#-*- coding: utf-8 -*-
'''
Created on 2015年9月21日

@author: jyb
'''
import petl
import psycopg2
import datetime
import types
import sys
import yaml
Config=yaml.load(file("config.yml"))


class Pg_Loader():
    """
        把文件中的data Load到postgresql中.
    """
    def __init__(self,
                 db_table ,
                 clear_cols ,
                 db=Config["database"]["db_name"],
                 db_user=Config["database"]["user"],
                 db_password=Config["database"]["password"],
                 db_host=Config["database"]["host"],
                 db_port=Config["database"]["port"],
                 file_data_path=None,
                 batch_rows_size=200,
                 load_type="etl"
                 ):
        
        self.file_data_path = file_data_path
        self.clear_cols = [i.strip() for i in clear_cols.split(',')]
        self.db=db
        self.db_user=db_user
        self.db_password=db_password
        self.db_table = db_table
        self.db_host = db_host
        self.db_port=db_port
        self.insert_buff = []
        self.batch_rows_size=batch_rows_size
#         self.mysql_db_engine = zpool[self.mysql_dburl]
        self.insert_rows_count = 0
        self.insert_rows_count_tmp = 0
        self.load_type=load_type
        self.file_header=''
        
    def load_file_to_pgtable(self):
        with open(self.file_data_path) as fr:
            flag = 0
            for line in fr:
                row = line.strip().split(',')
                if not flag:
                    flag = 1 #忽略首行
                    self.file_header=row
                    continue
                self.change_row_data_str_to_int(row)
                self.batch_insert_to_pg(row)
            self.insert_to_pg(self.insert_buff)
            self.insert_buff=[]
    
    def load_list_to_pgtable(self,data_list):
        if not data_list or not len(data_list):
            return None
        for line in data_list:
            row = line.strip().split('\t')
            self.change_row_data_str_to_int(row)
            self.batch_insert_to_pg(row)
        self.insert_to_pg(self.insert_buff)
        self.insert_buff=[]
    
    def batch_insert_to_pg(self,row):
        if len(self.insert_buff) < self.batch_rows_size:
            self.insert_buff.append(row)
        else:
            self.insert_buff.append(row)
            print "insert to db:",len(self.insert_buff)
            self.insert_to_pg(self.insert_buff)
            self.insert_buff=[]
            
    def insert_to_pg(self,buffer,retrytimes=3):
        for i in range(retrytimes):
            try:
                self.insert_rows(buffer)
                return True
            except Exception,e:
                import traceback
                ex=traceback.format_exc()
                print"[ERROR]", e
                print ex
                pass
        print "RETRY {0} TIMES FAILED".format(retrytimes)
    
        
    def insert_rows(self,rows):
        if self.load_type and self.load_type == "etl":
            self.load_by_etl(rows)
        else:
            self.load_by_sql(rows)
            
            
    def load_by_sql(self,rows):
        pass
            
    def load_by_etl(self,rows):
        db_tab_vals=[]
        tab_col_name=self.file_header
        if not tab_col_name:
            return None
        db_tab_vals.append(tab_col_name)
        if not rows or not len(rows):
            return None
        for row in rows:
            db_tab_vals.append(row)
        petl.appenddb(db_tab_vals,self.getConn(),self.db_table)
        print 'table',self.db_table
    
    def excute_by_sql(self,str_sql):
        try:
            conn = self.getConn()
            cur = conn.cursor()
            if not str_sql:
                return 
            result=cur.execute(str_sql)
            conn.commit()
        except Exception,e:
            print e
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
                
    def clear_db(self,conditions):
        """params: 'condition1,condition2,condition3...'  """
        if not conditions or not len(conditions):
            return None
        conds = [i.strip() for i in conditions.strip().split(",")]
        if not conds or len(conds) != len(self.clear_cols):
            return None
        where_condition = " "
        clear_col_ = self.clear_cols[:]
        clear_col_.reverse()
        for i in conds:
            where_condition += " "
            where_condition += clear_col_.pop()
            where_condition += " = "
            if type(i) is types.IntType:
                where_condition += str(i)
            if type(i) is types.StringType:
                where_condition += "'{0}'".format(i)
            where_condition += " and "
        where_condition += "1=1"
        clear_sql="delete from  \"{0}\" where {1}".format(self.db_table,where_condition)
        self.excute_by_sql(clear_sql)
        return True
              
    def change_row_data_str_to_int(self,row):
        for i in row:
            if type(i) is types.IntType:
                continue
            index=row.index(i)
            try:
                int_i=int(i)
                row.remove(i)
                row.insert(index, int_i)
            except:
                pass
            
    def getConn(self):           
        conn=psycopg2.connect(database=self.db, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port)
        return conn
    
def load(t_type,day,table_name,version,hour):
        d = datetime.datetime.strptime(day,"%Y%m%d")
        yearmonth = datetime.datetime.strftime(d,"%Y%m")
        yearmonthday = datetime.datetime.strftime(d,"%Y_%m_%d")
        file_path=''
        clear_condition=''
        
        if t_type == 'day' and version == 'old':
            file_path_prefix=Config["old_version"]["day_facts_file_path"]
            clear_condition=day
        elif t_type == 'hour' and version == 'old':
            file_path_prefix=Config["old_version"]["hour_facts_file_path"]
            clear_condition=day+','+hour
        elif t_type == 'day':
            file_path_prefix=Config["day_facts_file_path"]
            clear_condition=day
        else:
            file_path_prefix=Config["hour_facts_file_path"]
            clear_condition=day+','+hour
        
        clear_cols=''
        if t_type == 'day':
            if table_name == Config["db_table"]["Ad_Facts_By_Day"]["table_name"]:
                file_path=file_path_prefix+"{0}/{1}_ad_facts_by_day.csv".format(yearmonth,yearmonthday)
                clear_cols= Config["db_table"]["Ad_Facts_By_Day"]["clear_cols"]
            elif table_name == Config["db_table"]["Hit_Facts_By_Day"]["table_name"]:
                file_path=file_path_prefix+"{0}/{1}_hit_facts_by_day.csv".format(yearmonth,yearmonthday)
                clear_cols= Config["db_table"]["Hit_Facts_By_Day"]["clear_cols"]
            elif table_name == Config["db_table"]["Reqs_Facts_By_Day"]["table_name"]:
                file_path=file_path_prefix+"{0}/{1}_reqs_facts_by_day.csv".format(yearmonth,yearmonthday)
                clear_cols= Config["db_table"]["Reqs_Facts_By_Day"]["clear_cols"]
        elif t_type == 'hour':
            if table_name == Config["db_table"]["Ad_Facts_By_Hour"]["table_name"]:
                file_path=file_path_prefix+"{0}/{1}_{2}_ad_facts_by_hour.csv".format(yearmonth,yearmonthday,hour)
                clear_cols= Config["db_table"]["Ad_Facts_By_Hour"]["clear_cols"]
            elif table_name == Config["db_table"]["Hit_Facts_By_Hour"]["table_name"]:
                file_path=file_path_prefix+"{0}/{1}_{2}_hit_facts_by_hour.csv".format(yearmonth,yearmonthday,hour)
                clear_cols= Config["db_table"]["Hit_Facts_By_Hour"]["clear_cols"]
            elif table_name == Config["db_table"]["Reqs_Facts_By_Hour"]["table_name"]:
                file_path=file_path_prefix+"{0}/{1}_{2}_reqs_facts_by_hour.csv".format(yearmonth,yearmonthday,hour)
                clear_cols= Config["db_table"]["Reqs_Facts_By_Hour"]["clear_cols"]
        
        loader=Pg_Loader(batch_rows_size=Config["petl"]["batch_insert_db_size"],
                         db_table=table_name,
                         clear_cols=clear_cols,
                         load_type="etl",
                         file_data_path=file_path)
        try:
            loader.clear_db(clear_condition)
            loader.load_file_to_pgtable()
        except Exception,e:
            print "ERROR:data2postgresql ",day,table_name,e
if __name__ == "__main__":
    t_type=sys.argv[1]
    day = sys.argv[2]
    table_name=sys.argv[3]
    version = sys.argv[4]
    hour=''
    if t_type == 'hour':
        hour=sys.argv[5]
        
    try:
        load(t_type, day, table_name, version, hour)
    except Exception,e:
        print "ERROR:data2postgresql ",day,table_name,e
        
