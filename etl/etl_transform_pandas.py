# encoding=utf-8

import os
import numpy as np
import pandas as pd
import yaml
import psycopg2 as psy
import datetime as dt 
import init_log
import logging
import sys

''' 读取各种配置配置 '''
config = yaml.load(open("config.yml"))
supply_config = config.get('supply')
demand_config = config.get('demand')
database_config = config.get('database')
table_config = config.get('db_table')
pandas_config = config.get('pandas')

''' 数据库配置 '''
DB_DATABASE = database_config.get('db_name')
DB_USER = database_config.get('user')
DB_PASSWORD = database_config.get('password')
DB_HOST = database_config.get('host')
DB_PORT = database_config.get('port')

'''Supply'''
SUPPLY_HEADER = supply_config.get('raw_header')
SUPPLY_HEADER_DTYPE = supply_config.get('raw_header_dtype')
SUPPLY_HIT_HOUR_HEADER = supply_config.get('agg_hour_header')
SUPPLY_HIT_DAY_HEADER = supply_config.get('agg_day_header') 
SUPPLY_REQS_HOUR_HEADER = supply_config.get('reqs_hour_header')
SUPPLY_REQS_DAY_HEADER = supply_config.get('reqs_day_header')

'''Demand'''
DEMAND_HEADER = demand_config.get('raw_header')
DEMAND_HEADER_DTYPE = demand_config.get('raw_header_dtype')
DEMAND_AD_HOUR_HEADER = demand_config.get('agg_hour_header')
DEMAND_AD_DAY_HEADER = demand_config.get('agg_day_header')

''' 分隔符 '''
INPUT_COLUMN_SEP = config.get('column_sep')
OUTPUT_COLUMN_SEP = config.get('output_column_sep')

''' 每批次处理数据条数(读取文件)) '''
READ_CSV_CHUNK = pandas_config.get('read_csv_chunk')
''' 每批次处理数据条数(插入数据库)'''
DB_COMMIT_CHUNK = pandas_config.get('db_commit_chunk')

'''table names'''
HIT_FACTS_BY_HOUR_TABLE_NAME = pandas_config.get('hit_facts_by_hour2')
REQS_FACTS_BY_HOUR_TABLE_NAME = pandas_config.get('reqs_facts_by_hour2')
AD_FACTS_BY_HOUR_TABLE_NAME = pandas_config.get('ad_facts_by_hour2')
AD_FACTS_BY_DAY_TABLE_NAME = pandas_config.get('ad_facts_by_day2')
HIT_FACTS_BY_DAY_TABLE_NAME = pandas_config.get('hit_facts_by_day2')
REQS_FACTS_BY_DAY_TABLE_NAME = pandas_config.get('reqs_facts_by_day2')

'''file paths'''
SUPPLY_CSV_FILE_PATH = config.get('supply_csv_file_path')  # "/data/ad2/supply/"
DEMAND_CSV_FILE_PATH = config.get('demand_csv_file_path')  # "/data/ad2/demand/"
HOUR_FACTS_FILE_PATH = config.get('hour_facts_file_path')  # "/data/facts/hour/"
DAY_FACTS_FILE_PATH = config.get('day_facts_file_path')  # "/data/facts/day/"

PLACEHOLDER = -9

LOG_FILE_PATH = pandas_config.get("log_config_path")
LOG = init_log.init(LOG_FILE_PATH, 'pandasEtlLogger')
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s [%(name)s] [%(funcName)s] %(message)s", ""))
LOG.addHandler(console_handler)

SUPPLY_CONDITION_RELATION = {
				'total':[
					('ad_slot_id', '!=', -1),
					('ad_card_id', '!=', -1)
				]
			}
DEMAND_CONDITION_RELATION = {
			'click':[
				('type', '==', 2)
			],
			'impressions_start_total':[
				('type', '==', 1),
				('second', '==', 0),
			],
			'impressions_finish_total':[
				('type', '==', 1),
				('second', '==', 3600),
			]
		}

class Etl_Transform_Pandas:
	# def __init__(self, file_path, names, group_item, date, hour):
	# type: supply_hit/supply_reqs/demand
	def __init__(self, trans_type, start_time, day_merge=False):
		self.exec_start_time = dt.datetime.now()
		LOG.info("started with transform type:" + trans_type + ", handle time:" + start_time)
		
		# TODO FIXME validate the params here
		
		self.is_hour = True
		self.is_day = False
		
		self.table_ids = ["date_id"]
		
		self.hour = None
		if trans_type.find('hour') != -1 :  # 每小时
			self.start_time = dt.datetime.strptime(start_time, "%Y%m%d.%H")
			self.hour = self.start_time.hour
			self.output_root_path = HOUR_FACTS_FILE_PATH
			self.table_ids.append("time_id")
		elif trans_type.find('day') != -1 :  # 每天
			self.output_root_path = DAY_FACTS_FILE_PATH
			self.hour_output_root_path = HOUR_FACTS_FILE_PATH
			self.is_hour = False
			self.is_day = True
			self.start_time = dt.datetime.strptime(start_time, "%Y%m%d")
		
		self.month_folder = str(self.start_time.year) + ("%02d" % self.start_time.month)
		self.date = int(self.month_folder + ("%02d" % self.start_time.day))
		
		if(trans_type.find('supply') != -1):  # supply
			self.names = SUPPLY_HEADER
			self.names_dtype = self.tran_header_dtype(SUPPLY_HEADER_DTYPE)
			self.root_path = SUPPLY_CSV_FILE_PATH
			self.file_suffix = '.product.supply.csv'
			self.condition_relation = SUPPLY_CONDITION_RELATION
		elif(trans_type.find('demand') != -1):  # demand
			self.names = DEMAND_HEADER
			self.names_dtype = self.tran_header_dtype(DEMAND_HEADER_DTYPE)
			self.root_path = DEMAND_CSV_FILE_PATH
			self.file_suffix = '.product.demand.csv'
			self.condition_relation = DEMAND_CONDITION_RELATION
		
		if trans_type == 'supply_hour_hit':
			self.group_item = SUPPLY_HIT_HOUR_HEADER
			self.table_name = HIT_FACTS_BY_HOUR_TABLE_NAME
			self.run_type = 'hit'
		elif trans_type == 'supply_hour_reqs':
			self.group_item = SUPPLY_REQS_HOUR_HEADER
			self.table_name = REQS_FACTS_BY_HOUR_TABLE_NAME
			self.run_type = 'reqs'
		elif trans_type == 'demand_hour_ad':
			self.group_item = DEMAND_AD_HOUR_HEADER
			self.table_name = AD_FACTS_BY_HOUR_TABLE_NAME
			self.run_type = 'ad'
		elif trans_type == 'supply_day_hit':
			self.group_item = SUPPLY_HIT_DAY_HEADER
			self.table_name = HIT_FACTS_BY_DAY_TABLE_NAME
			self.run_type = 'hit'
		elif trans_type == 'supply_day_reqs':
			self.group_item = SUPPLY_REQS_DAY_HEADER
			self.table_name = REQS_FACTS_BY_DAY_TABLE_NAME
			self.run_type = 'reqs'
		elif trans_type == 'demand_day_ad':
			self.group_item = DEMAND_AD_DAY_HEADER
			self.table_name = AD_FACTS_BY_DAY_TABLE_NAME
			self.run_type = 'ad'
		
		if not os.path.exists(self.output_root_path + self.month_folder):
			os.makedirs(self.output_root_path + self.month_folder)
		self.output_file_path = self.output_root_path + self.month_folder + os.sep + start_time + "." + self.run_type + self.file_suffix
		
		self.tmp_path = self.output_file_path + ".tmp"
		self.init_info = {
			'is_hour': self.is_hour,
			'is_day': self.is_day,
			'start_time': self.start_time,
			'month_folder': self.month_folder,
			'date': self.date,
			'hour':self.hour,
			'root_path': self.root_path,
			'output_file_path': self.output_file_path,
			'file_suffix': self.file_suffix,
			'group_item': self.group_item,
			'table_name': self.table_name,
			'names': self.names,
			'names_dtype': self.names_dtype,
			'condition_relation': self.condition_relation,
			'tmp_file' : self.tmp_path
		}
		
		LOG.info('params init completed : ' + str(self.init_info))
		
		self.load_start_time = dt.datetime.now()
		try:
			is_load_success = self.load_files(start_time, day_merge)
			self.load_end_time = dt.datetime.now()
			if is_load_success:
				self.insert_start_time = dt.datetime.now()
				self.insert()
				self.insert_end_time = dt.datetime.now()
			else:
				LOG.error("load file failed,exit.")
		except Exception as e:
			LOG.error(e.message)
		self.exec_end_time = dt.datetime.now()
		
		exec_takes = self.exec_end_time - self.exec_start_time
		load_takes = self.load_end_time - self.load_start_time
		insert_takes = self.insert_end_time - self.insert_start_time
		
		LOG.info("process complete in [" + str(exec_takes.seconds) + "] seconds (" + str(exec_takes.seconds / 60) + " minutes), inside load file and transform takes " 
				+ str(load_takes.seconds) + " seconds, and insert to DB takes " + str(insert_takes.seconds) + "seconds")
	def load_files(self, start_time_str, day_merge):
		if os.path.exists(self.tmp_path):
			LOG.warn("tmp file already exists,remove")
			os.remove(self.tmp_path)
		
		# 数据集数组，按小时计算只会有一个，按天计算每个文件会产生一个，最多24个数据集
		# self.dfs = []
		file_path = None
		df = None
		if self.is_hour:
			# 20150901.07.product.supply.csv
			file_path = self.root_path + self.month_folder + os.sep + start_time_str + self.file_suffix
			LOG.info('load file:' + file_path)
			if os.path.exists(file_path):
				# 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
				df = pd.read_csv(file_path, sep=INPUT_COLUMN_SEP, names=self.names, header=None, chunksize=READ_CSV_CHUNK, index_col=False)
				self.transform_section(df)
			else:
				return False
		elif self.is_day:
			count = 0
			if not day_merge:  # 重新计算所有小时数
				parent_path = self.root_path + self.month_folder + os.sep
				LOG.info('load dir:' + parent_path)
				file_name_contain_day = str(self.date)
				file_generator = os.walk(parent_path).next()
				if file_generator is not None:
					for file_name in file_generator[2]:
						file_path = parent_path + file_name
						if(os.path.isfile(file_path) 
							and file_name.find(file_name_contain_day) != -1
								and str(file_name).endwith(self.file_suffix)):
							# 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
							LOG.info('load file:' + file_path)
							df = pd.read_csv(file_path, sep=INPUT_COLUMN_SEP, names=self.names, header=None, chunksize=READ_CSV_CHUNK, index_col=False)
							LOG.info('handel file:' + file_path)
							count = count + 1
							self.transform_section(df)
			else:  # 寻找已经计算过的小时数据合并
				for h24 in range(0, 24):
					hour_file_path = self.hour_output_root_path + self.month_folder + os.sep + str(self.date) + (".%02d" % h24) + "." + self.run_type + self.file_suffix
					if(os.path.isfile(hour_file_path)):
							# 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
							LOG.info('load file:' + hour_file_path)
							# coppy group_item 复制复本
							sum_names = []
							for item in self.group_item:
								sum_names.append(item)
							for key in self.condition_relation.keys():
								sum_names.append(key)
							df = pd.read_csv(hour_file_path, sep=OUTPUT_COLUMN_SEP, names=sum_names, header=None, index_col=False)
							LOG.info('merge file:' + hour_file_path)
							df.to_csv(self.tmp_path, sep=OUTPUT_COLUMN_SEP, header=False, na_rep='0', mode="a")
							count = count + 1
					else:
						LOG.warn("merge file not exists:" + hour_file_path)
			if count == 0:
				return False
		return True
	'''转换配置里的数据类型'''
	def tran_header_dtype(self, dtype_dict):
		target = {}
		for dt in dtype_dict.iteritems():
			k = dt[0]
			t = dt[1]
			if t == 'int':
				target[k] = np.int64
			elif t == 'string':
				target[k] = np.string0
		return target

	# 返回占位数据
	def get_init_data(self, group_item, key):
		obj = {}
		for gi in group_item:
			if self.names_dtype[gi] == np.int64:
				obj[gi] = [PLACEHOLDER]
			elif self.names_dtype[gi] == np.string0:
				obj[gi] = [str(PLACEHOLDER)]
		obj[key] = [0]
		return pd.DataFrame(obj)
		
	def transform_section(self, df):
		LOG.info('transform...')
		###############遍历各个分段，分段数据第一次Group Count后存入临时文件#############
		
		# for df in self.dfs:
		for chunk in df:
			grouped = None
			for item in self.condition_relation.items():
				column_name = item[0]
				relations = item[1]
				LOG.info("filter column: " + column_name)
				tmp_chunk = chunk
				
				for rel in relations:
					key = rel[0]
					opt = rel[1]
					val = rel[2]
					if '==' == opt:
						LOG.info("filter column: " + column_name + "," + key + "==" + str(val))
						tmp_chunk = tmp_chunk[tmp_chunk[key] == val]
					elif '!=' == opt:
						LOG.info("filter column: " + column_name + "," + key + "!=" + str(val))
						tmp_chunk = tmp_chunk[tmp_chunk[key] != val]
				LOG.info("merge column result: " + column_name)
				
				if len(tmp_chunk) == 0:
					tmp_chunk = self.get_init_data(self.group_item, column_name)
				if grouped is None:
					grouped = tmp_chunk.groupby(self.group_item).size()
				else:
					grouped = pd.concat([grouped, tmp_chunk.groupby(self.group_item).size()], axis=1)

			# 转换为DataFrame
			groupframe = pd.DataFrame(grouped)
			# 保存到临时CSV文件
			groupframe.to_csv(self.tmp_path, sep=OUTPUT_COLUMN_SEP, header=False, na_rep='0', mode="a")
			
			# info info
			LOG.info("grouped: " + str(len(chunk)) + " records")
		
		LOG.info("save to tmp file : " + self.tmp_path)
		###############遍历各个分段，分段数据第一次Group Count后存入临时文件############
		LOG.info ('transform!')
	
	# 读取CSV文件插入数据库
	def insert(self):
		
		# coppy group_item 复制复本
		sum_names = []
		for item in self.group_item:
			sum_names.append(item)
			
		for key in self.condition_relation.keys():
			sum_names.append(key)
		
		##############从临时文件读取数据计算最终结果，Group Sum####################
		# info info
		LOG.info("merge the tmp file : " + self.tmp_path)
		# 输出结果到文件
		df = pd.read_csv(self.tmp_path, sep=OUTPUT_COLUMN_SEP, names=sum_names, header=None, dtype=self.names_dtype)
		df.dropna()
		total_grouped = df.groupby(self.group_item).sum()
		##############从临时文件读取数据计算最终结果，Group Sum####################
		
		# defub info
		LOG.info("merge result size:" + str(len(total_grouped)))
		
		total_groupframe = pd.DataFrame(total_grouped)
		LOG.info("write result to csv file:" + self.output_file_path)
		total_groupframe.to_csv(self.output_file_path, sep=OUTPUT_COLUMN_SEP, header=False)
		
		LOG.info("remove tmp file:" + self.tmp_path)
		# 删除临时文件
		os.remove(self.tmp_path)
		
		# info info
		LOG.info("connect --> db:" + str(DB_DATABASE) + ",user:" + str(DB_USER) + ",password:***,host:" + str(DB_HOST) + ",port:" + str(DB_PORT) + "...")
		conn = psy.connect(database=DB_DATABASE, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
		cur = conn.cursor()
		
		#################清空表######################
		sql = 'DELETE FROM "' + self.table_name + '" WHERE '
		
		insert_column = ''
		value_str = ''
		
		for tid in self.table_ids:
			if(self.table_ids.index(tid) > 0):
				sql = sql + " AND "
				insert_column = insert_column + ","
				value_str = value_str + ","
			sql = sql + " " + tid + " = %s"
			insert_column = insert_column + '"' + tid + '"'
			value_str = value_str + '%s'
		sql = sql + ";"
		
		LOG.info ("prepare delete:" + sql + str(self.date) + "," + str(self.hour))
		del_val = [self.date]
		if(self.is_hour):
			del_val.append(self.hour)
		cur.execute(sql, del_val)
		conn.commit()
		#################清空表######################
		
		#################拼接Insert SQL、组装Insert值######################
		for name in self.group_item:
			insert_column = insert_column + ',"' + str(name) + '"'
			value_str = value_str + ',%s'
		for key in self.condition_relation.keys():
			insert_column = insert_column + ',"' + key + '"'
			value_str = value_str + ',%s'
		sql = 'INSERT INTO "' + self.table_name + '"(' + insert_column + ') VALUES (' + value_str + ');'
		sql_count = 0
		tg_count = str(len(total_groupframe))
		LOG.info("prepare insert : " + sql)
		#################拼接Insert SQL、组装Insert值######################
		
		#################分段提交数据######################################
		value_list = []
		for index, row in total_groupframe.iterrows():
			is_tuple = type(index) == tuple
			value = [self.date]
			if(self.is_hour):
				value.append(self.hour)
			is_break = False;
			for name in self.group_item:
				if is_tuple:
					v = int(index[self.group_item.index(name)])
				else:
					v = int(index)
				
				if v == PLACEHOLDER or v == str(PLACEHOLDER):  # 表示是占位行
					is_break = True
					break
				
				value.append(v)
			if is_break:  # 跳过占位行
				continue
			for r in row:  # 这里是计算的值，顺序应当是和condition_relation里的Key对应的
				value.append(int(r))
					
			value_list.append(value)
			
			sql_count = sql_count + 1
			
			# 每DB_COMMIT_CHUNK条提交一次
			if sql_count % DB_COMMIT_CHUNK == 0:
				# info info
				LOG.info("commit " + str(sql_count) + "/" + tg_count + " records")
				cur.executemany(sql, value_list)
				conn.commit()
				value_list = []
			
		LOG.info("commit all...")
		if len(value_list) != 0:
			cur.executemany(sql, value_list)
			conn.commit()
		LOG.info("commit all!")
		#################分段提交数据######################################
		cur.close()
		conn.close()
if __name__ == "__main__":
	cop_type = sys.argv[1]  # 'supply_day_hit'
	time = sys.argv[2]  # '20150923'
	
	if 'old' == sys.argv[3]:
		config_old = config.get('old_version')
		supply_config = config_old.get('supply')
		demand_config = config_old.get('demand')
		
		'''Supply'''
		SUPPLY_HEADER = supply_config.get('raw_header')
		SUPPLY_HEADER_DTYPE = supply_config.get('raw_header_dtype')
		SUPPLY_HIT_HOUR_HEADER = supply_config.get('agg_hour_header')
		SUPPLY_HIT_DAY_HEADER = supply_config.get('agg_day_header') 
		SUPPLY_REQS_HOUR_HEADER = supply_config.get('reqs_hour_header')
		SUPPLY_REQS_DAY_HEADER = supply_config.get('reqs_day_header')
		
		'''Demand'''
		DEMAND_HEADER = demand_config.get('raw_header')
		DEMAND_HEADER_DTYPE = demand_config.get('raw_header_dtype')
		DEMAND_AD_HOUR_HEADER = demand_config.get('agg_hour_header')
		DEMAND_AD_DAY_HEADER = demand_config.get('agg_day_header')
	
		SUPPLY_CSV_FILE_PATH = config_old.get('supply_csv_file_path')  # "/data/ad2/supply/"
		DEMAND_CSV_FILE_PATH = config_old.get('demand_csv_file_path')  # "/data/ad2/demand/"
		HOUR_FACTS_FILE_PATH = config_old.get('hour_facts_file_path')  # "/data/facts/hour/"
		DAY_FACTS_FILE_PATH = config_old.get('day_facts_file_path')  # "/data/facts/day/"
		'''
		SUPPLY_CSV_FILE_PATH = "F:\\data\\ad2\\supply\\" # config_old.get('supply_csv_file_path')  # "/data/ad2/supply/"
		DEMAND_CSV_FILE_PATH = config_old.get('demand_csv_file_path')  # "/data/ad2/demand/"
		HOUR_FACTS_FILE_PATH = "F:\\data\\facts\\hour\\"  #config_old.get('hour_facts_file_path')  # "/data/facts/hour/"
		DAY_FACTS_FILE_PATH = "F:\\data\\facts\\day\\" # config_old.get('day_facts_file_path')  # "/data/facts/day/"
		'''
	day_merge = False
	if len(sys.argv) > 4 :
		day_merge = sys.argv[4] == 'merge'
	
	Etl_Transform_Pandas(cop_type, time, day_merge)  # .compute()
		
	# new_etp = Etl_Transform_Pandas('supply_hour_hit', '20150923.04')
	# LOG.info("Welcome to Etl_Transform_Pandas. Version : 0.1.1  code by Dico:dingzheng@imgo.tv")
	# new_etp = Etl_Transform_Pandas('demand_hour_ad', '20150930.05')
	# new_etp.compute()
