#encoding=utf-8
import os
import numpy as np
import pandas as pd
import yaml
import psycopg2 as psy

''' 数据库配置 '''
DB_DATABASE="adc"
DB_USER="postgres"
DB_PASSWORD=None
DB_HOST="10.100.5.80"
DB_PORT="11921"

''' 读取配置 '''
config = yaml.load(open("config.yml"))
supply_config = config.get('supply')
demand_config = config.get('demand')

SUPPLY_HEADER = supply_config.get('raw_header')
SUPPLY_AGG_HEADER = supply_config.get('agg_header')

DEMAND_HEADER = demand_config.get('raw_header')
DEMAND_AGG_HEADER = demand_config.get('agg_header')

''' 分隔符 '''
COLUMN_SEP = config.get('column_sep')
OUTPUT_COLUMN_SEP = config.get('output_column_sep')
''' 每批次处理数据条数 '''
CHUNK = 50000

class Etl_Transform_Pandas:
	def __init__(self,file_path,sep,names,groupItem,date,hour):
		self.groupItem = groupItem
		self.names = names
		self.date = date
		self.hour = hour
		# 分段处理CSV文件，每CHUNK行读取一次
		self.df=pd.read_csv(file_path,sep=sep,names=names,header=None,chunksize=CHUNK)
		self.total_groupframe=None
	# 分段转换数据并存储到CSV文件
	def transform_section(self,path,sep):
		is_create = True
		tmp_path = path+".tmp"
		for chunk in self.df:
			# 过滤掉 -1
			chunk_thin=chunk[(chunk.slotid!=-1)&(chunk.cardid!=-1)]
			
			# group
			grouped = chunk_thin.groupby(self.groupItem).size()
			# 转换为DataFrame
			groupframe=pd.DataFrame(grouped)
			
			# 保存到临时CSV文件
			if(is_create):
				groupframe.to_csv(tmp_path,sep=sep,header=False)
				is_create = False
			else:
				groupframe.to_csv(tmp_path,sep=sep,header=False,mode="a")
			
			# debug info
			print("groupd: "+str(len(chunk_thin))+"/"+str(len(chunk))+" records")
		
		# 读取数据，聚合
		
		# coppy groupItem
		sum_names=[]
		for item in self.groupItem:
			sum_names.append(item)
		sum_names.append('total')
		
		# defub info
		print("merge the tmp file...")
		
		name_dtype={}
		for name in sum_names:
			name_dtype[name]=np.int64
		df=pd.read_csv(tmp_path,sep=sep,names=sum_names,header=None,dtype=name_dtype)
		
		total_grouped=df.groupby(self.groupItem).sum()
		
		# defub info
		print("merge result size:"+str(len(total_grouped)))
		
		self.total_groupframe=pd.DataFrame(total_grouped)
		self.total_groupframe.to_csv(path,sep=sep,header=False)
		
		# 删除临时文件
		os.remove(tmp_path)
		
	#读取CSV文件插入数据库
	def insert_hit_facts_by_hour(self,file_path,sep):
		conn = psy.connect(database=DB_DATABASE,user=DB_USER,password=DB_PASSWORD,host=DB_HOST,port=DB_PORT)
		cur = conn.cursor()
		sql='INSERT INTO "public"."Hit_Facts_By_Hour2"("date_id","time_id","ad_card_id","ad_slot_id","ad_create_id","total") VALUES (%s,%s,%s,%s,%s,%s);'
		
		sql_count = 0
		tg_count = str(len(self.total_groupframe))
		for index,row in self.total_groupframe.iterrows():
			sql_count = sql_count+1
			slotid=int(index[0])
			cardid=int(index[1])
			creativeid=int(index[2])
			total=int(row[0])
			cur.execute(sql,(self.date,self.hour,cardid,slotid,creativeid,total))
			# 每500条提交一次
			if sql_count % 500 == 0:
				# debug info
				print("commit " + str(sql_count) + "/" + tg_count + " records")
				
				conn.commit()
		print("commit all")
		conn.commit()
		cur.close()
		conn.close()
if __name__ == "__main__":
	supply_file_path = r'C:\Users\Administrator\Desktop\20150920.04.product.supply.csv'
	supply_out_path = r'C:\Users\Administrator\Desktop\20150920.04.product.supply.result.csv'
	supply_etp = Etl_Transform_Pandas(supply_file_path,COLUMN_SEP,SUPPLY_HEADER,SUPPLY_AGG_HEADER,'20150920','04')
	supply_etp.transform_section(supply_out_path,OUTPUT_COLUMN_SEP)
	supply_etp.insert_hit_facts_by_hour(supply_out_path,OUTPUT_COLUMN_SEP)
	