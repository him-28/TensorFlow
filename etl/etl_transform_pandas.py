#encoding=utf-8

import os
import numpy as np
import pandas as pd
import yaml
import psycopg2 as psy


''' 读取各种配置配置 '''
config = yaml.load(open("config.yml"))
supply_config = config.get('supply')
demand_config = config.get('demand')
database_config = config.get('database')
table_config = config.get('db_table')
pandas_config = config.get('pandas')

''' 数据库配置 '''
DB_DATABASE=database_config.get('db_name')
DB_USER=database_config.get('user')
DB_PASSWORD=database_config.get('password')
DB_HOST=database_config.get('host')
DB_PORT=database_config.get('port')

'''Supply'''
SUPPLY_HEADER = supply_config.get('raw_header')
SUPPLY_HIT_HOUR_HEADER = supply_config.get('agg_hour_header')
SUPPLY_REQS_HOUR_HEADER = supply_config.get('reqs_hour_header')
SUPPLY_REQS_HIT_HOUR_HEADER = supply_config.get('reqs_hour_header')

'''Demand'''
DEMAND_HEADER = demand_config.get('raw_header')
DEMAND_AD_HOUR_HEADER = demand_config.get('agg_hour_header')

''' 分隔符 '''
INPUT_COLUMN_SEP = config.get('column_sep')
OUTPUT_COLUMN_SEP = config.get('output_column_sep')

''' 每批次处理数据条数(读取文件)) '''
READ_CSV_CHUNK = pandas_config.get('read_csv_chunk')
''' 每批次处理数据条数(插入数据库)'''
DB_COMMIT_CHUNK = pandas_config.get('db_commit_chunk')
'''hit_facts_by_hour2'''
HIT_FACTS_BY_HOUR_TABLE_NAME=pandas_config.get('hit_facts_by_hour2')
REQS_FACTS_BY_HOUR_TABLE_NAME=pandas_config.get('reqs_facts_by_hour2')
AD_FACTS_BY_HOUR_TABLE_NAME = pandas_config.get('ad_facts_by_hour2')

PLACEHOLDER=-9

class Etl_Transform_Pandas:
	def __init__(self,file_path,names,groupItem,date,hour):
		self.groupItem = groupItem
		self.names = names
		self.date = date
		self.hour = hour
		# 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
		self.df=pd.read_csv(file_path,sep=INPUT_COLUMN_SEP,names=names,header=None,chunksize=READ_CSV_CHUNK)
		self.total_groupframe=None
		
	'''
	分段转换数据并存储到CSV文件
	'''
	def transform_supply_section(self,output_path,the_type):
		print 'transform...'
		
		###############遍历各个分段，分段数据第一次Group Count后存入临时文件#############
		is_create = True
		tmp_path = output_path+".tmp"
		for chunk in self.df:
			chunk_thin=chunk
			if the_type=='hit':
				# 过滤掉 -1
				chunk_thin=chunk[(chunk['ad_slot_id']!=-1)&(chunk['ad_card_id']!=-1)]
			# group
			grouped = chunk_thin.groupby(self.groupItem).size()
			# 转换为DataFrame
			groupframe=pd.DataFrame(grouped)
			
			# 保存到临时CSV文件
			if(is_create):
				groupframe.to_csv(tmp_path,sep=OUTPUT_COLUMN_SEP,header=False)
				is_create = False
			else:
				groupframe.to_csv(tmp_path,sep=OUTPUT_COLUMN_SEP,header=False,mode="a")
			
			# debug info
			print("groupd: "+str(len(chunk_thin))+" in "+str(len(chunk))+" records")
		
		###############遍历各个分段，分段数据第一次Group Count后存入临时文件############
		
		
		# coppy groupItem 复制复本
		sum_names=[]
		for item in self.groupItem:
			sum_names.append(item)
		sum_names.append('total')
		
		
		##############从临时文件读取数据计算最终结果，Group Sum####################
		# defub info
		print("merge the tmp file...")
		name_dtype={}
		for name in sum_names:
			name_dtype[name]=np.int64 # TODO FIXME read from properties file
		# 输出结果到文件
		df=pd.read_csv(tmp_path,sep=OUTPUT_COLUMN_SEP,names=sum_names,header=None,dtype=name_dtype)
		df.dropna()
		total_grouped=df.groupby(self.groupItem).sum()
		##############从临时文件读取数据计算最终结果，Group Sum####################
		
		
		# defub info
		print("merge result size:"+str(len(total_grouped)))
		
		self.total_groupframe=pd.DataFrame(total_grouped)
		self.total_groupframe.to_csv(output_path,sep=OUTPUT_COLUMN_SEP,header=False)
		
		# 删除临时文件
		os.remove(tmp_path)
		
		print 'transform!'

	# 返回占位数据
	def getInitData(self,groupItem,key):
		obj = {}
		for gi in groupItem:
			obj[gi] = [PLACEHOLDER]
		obj[key] = [0]
		return pd.DataFrame(obj)
		
	def transform_section(self,output_path,condition_relations):
		print 'transform...'
		
		###############遍历各个分段，分段数据第一次Group Count后存入临时文件#############
		is_create = True
		tmp_path = output_path+".tmp"
		for chunk in self.df:
			grouped = None
			for item in condition_relations.items():
				column_name=item[0]
				relations=item[1]
				print("filter column: "+column_name)
				tmp_chunk = chunk
				
				for rel in relations:
					key = rel[0]
					opt = rel[1]
					val = rel[2]
					if '==' == opt:
						print("filter column: "+ column_name + "," + key + "==" + str(val))
						tmp_chunk = tmp_chunk[tmp_chunk[key]==val]
					elif '!=' == opt:
						print("filter column: "+column_name + "," + key + "!=" + str(val))
						tmp_chunk = tmp_chunk[tmp_chunk[key]!=val]
				
				print("merge column result: " + column_name)
				
				if len(tmp_chunk)==0:
					tmp_chunk=self.getInitData(self.groupItem,column_name)
				
				if grouped is None:
					grouped = tmp_chunk.groupby(self.groupItem).size() # TODO 确认这里是否需要深度Copy
				else:
					grouped = pd.concat([grouped,tmp_chunk.groupby(self.groupItem).size()],axis=1)

			# 转换为DataFrame
			groupframe=pd.DataFrame(grouped)
			# 保存到临时CSV文件
			if(is_create):
				groupframe.to_csv(tmp_path,sep=OUTPUT_COLUMN_SEP,header=False,na_rep='0')
				is_create = False
			else:
				groupframe.to_csv(tmp_path,sep=OUTPUT_COLUMN_SEP,header=False,na_rep='0',mode="a")
			
			# debug info
			print("grouped: "+str(len(chunk))+" records")
		print("save tmp file to : " + tmp_path)
		###############遍历各个分段，分段数据第一次Group Count后存入临时文件############
		
		
		# coppy groupItem 复制复本
		sum_names=[]
		for item in self.groupItem:
			sum_names.append(item)
			
		for key in condition_relations.keys():
			sum_names.append(key)
		
		##############从临时文件读取数据计算最终结果，Group Sum####################
		# defub info
		print("merge the tmp file...")
		name_dtype={}
		for name in sum_names:
			name_dtype[name]=np.int64 # TODO FIXME read from properties file
		# 输出结果到文件
		df=pd.read_csv(tmp_path,sep=OUTPUT_COLUMN_SEP,names=sum_names,header=None,dtype=name_dtype)
		df.dropna()
		total_grouped=df.groupby(self.groupItem).sum()
		##############从临时文件读取数据计算最终结果，Group Sum####################
		
		# defub info
		print("merge result size:"+str(len(total_grouped)))
		
		self.total_groupframe=pd.DataFrame(total_grouped)
		self.total_groupframe.to_csv(output_path,sep=OUTPUT_COLUMN_SEP,header=False)
		
		# 删除临时文件
		# os.remove(tmp_path)
		
		print 'transform!'
	# 读取CSV文件插入数据库
	def supply(self,supply_out_path,the_type):
		print "supply application started."
		
		condition_relation = {
				'total':[
					('ad_slot_id','!=',-1),
					('ad_card_id','!=',-1)
				]
			}
		
		# self.transform_supply_section(supply_out_path,the_type)
		self.transform_section(supply_out_path,condition_relation)
		
		# SUPPLY_HIT_HOUR_HEADER.append('total')
		if the_type=='hit':
			self.insert(supply_out_path,HIT_FACTS_BY_HOUR_TABLE_NAME,SUPPLY_HIT_HOUR_HEADER,condition_relation)
		elif the_type=='reqs':
			self.insert(supply_out_path,REQS_FACTS_BY_HOUR_TABLE_NAME,SUPPLY_REQS_HIT_HOUR_HEADER,condition_relation)
		print "supply application ended."
	def demand(self,demand_out_path):
		print "demand application started."
		
		condition_relation = {
			'click':[
				('type','==',2)
			],
			'impressions_start_total':[
				('type','==',1),
				('second','==',0),
			],
			'impressions_finish_total':[
				('type','==',1),
				('second','==',3600),
			]
		}
		
		
		self.transform_section(demand_out_path,condition_relation)
		self.insert(demand_out_path,AD_FACTS_BY_HOUR_TABLE_NAME,DEMAND_AD_HOUR_HEADER,condition_relation)
		print "demand application ended."
		
	# 读取CSV文件插入数据库
	def insert(self,file_path,table_name,column_names,condition_relation):
		# debug info
		print "connect --> db:"+str(DB_DATABASE)+",user:"+str(DB_USER)+",password:***,host:"+str(DB_HOST)+",port:"+str(DB_PORT)+"..."
		conn = psy.connect(database=DB_DATABASE,user=DB_USER,password=DB_PASSWORD,host=DB_HOST,port=DB_PORT)
		cur = conn.cursor()
		
		#################清空表######################
		sql='DELETE FROM "'+table_name+'" WHERE "date_id"=%s AND "time_id"=%s;'
		print "prepare delete:" + sql, self.date,self.hour
		cur.execute(sql,(self.date,self.hour))
		conn.commit()
		#################清空表######################
		
		
		#################拼接Insert SQL、组装Insert值######################
		insert_column='"date_id","time_id"'
		value_str='%s,%s'
		for name in column_names:
			insert_column = insert_column + ',"'+str(name)+'"'
			value_str=value_str+',%s'
		for key in condition_relation.keys():
			insert_column = insert_column + ',"'+key+'"'
			value_str=value_str+',%s'
		sql='INSERT INTO "'+table_name+'"('+insert_column+') VALUES ('+value_str+');'
		sql_count = 0
		tg_count = str(len(self.total_groupframe))
		print "prepare sql : " + sql
		#################拼接Insert SQL、组装Insert值######################
		
		#################分段提交数据######################################
		value_list = []
		for index,row in self.total_groupframe.iterrows():
			is_tuple = type(index)==tuple
			value=[self.date,self.hour]
			is_break = False;
			for name in column_names:
				if is_tuple:
					v = int(index[self.groupItem.index(name)])
				else:
					v=int(index)
				
				if v==PLACEHOLDER or v == str(PLACEHOLDER): # 表示是占位行
					is_break = True
					break
				
				value.append(v)
			if is_break: # 跳过占位行
				continue
			for r in row: # 这里是计算的值，顺序应当是和condition_relation里的Key对应的
				value.append(int(r))
					
			value_list.append(value)
			
			sql_count = sql_count+1
			
			# 每DB_COMMIT_CHUNK条提交一次
			if sql_count % DB_COMMIT_CHUNK == 0:
				# debug info
				print("commit " + str(sql_count) + "/" + tg_count + " records")
				cur.executemany(sql,value_list)
				conn.commit()
				value_list = []
			
		print("commit all...")
		if len(value_list)!=0:
			cur.executemany(sql,value_list)
			conn.commit()
		print("commit all!")
		#################分段提交数据######################################
		
		cur.close()
		conn.close()
if __name__ == "__main__":
	
	etp_hit = Etl_Transform_Pandas(r'F:\20150930.04.product1.demand.csv',DEMAND_HEADER,DEMAND_AD_HOUR_HEADER,'20150930','04')
	etp_hit.demand(r'F:\20150930.04.product1.demand.ad.csv')
	
	'''
	etp_hit = Etl_Transform_Pandas(r'F:\20150923.10.product.supply.csv',SUPPLY_HEADER,SUPPLY_HIT_HOUR_HEADER,'20150923','10')
	etp_hit.supply(r'F:\20150923.10.product.supply.hit.csv','hit')
	
	etp_hit = Etl_Transform_Pandas(r'F:\20150923.10.product.supply.csv',SUPPLY_HEADER,SUPPLY_HIT_HOUR_HEADER,'20150923','10')
	etp_hit.supply(r'F:\20150923.10.product.supply.hit.csv','hit')
	
	etp_reqs = Etl_Transform_Pandas(r'F:\20150923.04.product.supply.csv',SUPPLY_HEADER,SUPPLY_REQS_HOUR_HEADER,'20150923','04')
	etp_reqs.supply(r'F:\20150923.04.product.supply.reqs.csv','reqs')

	etp_hit = Etl_Transform_Pandas(r'F:\20150901.12.product.supply.csv',SUPPLY_HEADER,SUPPLY_HIT_HOUR_HEADER,'20150920','12')
	etp_hit.supply(r'F:\20150901.12.product.supply.hit.csv','hit')
	
	etp_reqs = Etl_Transform_Pandas(r'F:\20150901.12.product.supply.csv',SUPPLY_HEADER,SUPPLY_REQS_HOUR_HEADER,'20150920','12')
	etp_reqs.supply(r'F:\20150901.12.product.supply.reqs.csv','reqs')
	'''
	