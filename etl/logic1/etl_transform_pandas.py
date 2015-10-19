# encoding=utf-8
'''
使用Pandas处理CSV文件数据
'''
import os
import numpy as np
import pandas as pd
import psycopg2 as psy
import datetime as dt
import logging
import sys

from etl.conf.settings import Config
from etl.util import init_log
LOG = init_log.init("util/logger.conf", 'pandasEtlLogger')


def tran_header_dtype(dtype_dict):
    '''转换配置里的数据类型'''
    target = {}
    for dtype in dtype_dict.iteritems():
        k = dtype[0]
        the_type = dtype[1]
        if the_type == 'int':
            target[k] = np.int64
        elif the_type == 'string':
            target[k] = np.string0
    return target

class Etl_Transform_Pandas:
    """使用Pandas处理CSV文件数据，初始化"""
    def __init__(self, day_merge=False, console_print=False):
        LOG.info("[Etl_Transform_Pandas] [" + str(day_merge) + "] [" + str(console_print) + "]")
        if console_print:  # print debug info in console
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            fmt_str = "[%(levelname)s] %(asctime)s [%(name)s] [%(funcName)s] %(message)s"
            console_handler.setFormatter(logging.Formatter(fmt_str, ""))
            LOG.addHandler(console_handler)
        self.day_merge = day_merge


        # 开始执行时间
        self.exec_start_time=None
        self.exec_end_time=None
        # 开始加载文件时间
        self.load_start_time=None
        self.load_end_time=None
        #开始插入数据库时间
        self.insert_start_time=None
        self.insert_end_time=None

        #处理文件的开始时间
        self.start_time=None
        #配置
        self.config=None
        #参数
        self.params = {}

    def configure(self, config_type):
        '''加载配置文件'''
        self.init_config(Config)
        if config_type == 'old': # 加载老版本配置，覆盖默认配置
            self.init_old_config(Config)
    def compute_old(self, trans_type, start_time):
        '''转换老版本数据'''
        LOG.info("load old version configure...")
        return self.compute(trans_type, start_time, 'old')

    def compute(self, trans_type, start_time, config_version='new'):
        '''transform datas'''
        #配置
        self.configure(config_version)

        #开始转换数据'''
        LOG.info("started with transform type:" + trans_type + ", handle time:" + start_time)

        # 验证参数是否正确
        if not self.validate_params(trans_type, start_time):
            LOG.error("wrong params,plz fix it.")
            return -1

        # 开始执行时间
        self.exec_start_time = dt.datetime.now()
        # 初始化
        self.init_params(trans_type, start_time)
        # 开始加载文件时间
        self.load_start_time = dt.datetime.now()
        insert_takes = 0
        try:
            is_load_success = self.load_files(start_time)
            self.load_end_time = dt.datetime.now()
            if is_load_success:
                self.insert_start_time = dt.datetime.now()
                self.insert()
                self.insert_end_time = dt.datetime.now()
                insert_takes = (self.insert_end_time - self.insert_start_time).seconds
            else:
                LOG.error("load file failed,exit.")
                return -1
        except Exception as exce:
            import traceback
            print traceback.format_exc()
            LOG.error(exce.message)
            return -1
        self.exec_end_time = dt.datetime.now()

        exec_takes = self.exec_end_time - self.exec_start_time
        load_takes = self.load_end_time - self.load_start_time

        LOG.info("process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                str(exec_takes.seconds / 60) + " minutes), inside load file and transform takes "\
                + str(load_takes.seconds) + " seconds, and insert to DB takes " + \
                str(insert_takes) + " seconds")
        return 0

    def validate_params(self, trans_type, start_time):
        '''验证参数是否正确'''
        if trans_type == 'supply_hour_hit' or trans_type == 'supply_hour_reqs'\
            or trans_type == 'demand_hour_ad' or trans_type == 'supply_day_hit'\
            or trans_type == 'supply_day_reqs' or trans_type == 'demand_day_ad':

            if trans_type.find('day') != -1:
                try:
                    self.start_time = dt.datetime.strptime(start_time, "%Y%m%d")
                    return True
                except TypeError:
                    LOG.error(start_time + ' need format as : %Y%m%d')
                except ValueError:
                    LOG.error(start_time + ' need format as : %Y%m%d')
            else:
                s_arr = start_time.split(".")
                if len(s_arr) != 2:
                    LOG.error(start_time + ' need format as : %Y%m%d.%H')
                    return False

                #24小时格式的hour
                the_hour = int(s_arr[1])

                # 把1~24小时格式时间格式化成0~23小时 Start
                # 拼接时间字符串
                start_time_format_in_23hours = s_arr[0] + "." + str(the_hour - 1)
                try:
                    #格式化
                    self.start_time = dt.datetime.strptime(start_time_format_in_23hours\
                                                        , "%Y%m%d.%H")
                    return True
                except TypeError:
                    LOG.error(start_time + ' need format as : %Y%m%d.%H')
                except ValueError:
                    LOG.error(start_time + ' need format as : %Y%m%d.%H')
                # 把1~24小时格式时间格式化成0~23小时 End

        else:
            LOG.error('can not handle the trans_type:' + trans_type + \
                    ' ,it should be one of them:[supply_hour_hit,supply_hour_reqs,\
                    demand_hour_ad,supply_day_hit,supply_day_reqs,demand_day_ad]')
        return False
    def init_old_config(self, config_file):
        '''初始化老版本配置'''

        LOG.info("load old version configure...")
        config_old = config_file.get('old_version')

        supply_config = config_old.get('supply')
        demand_config = config_old.get('demand')

        #Supply'''
        self.config['SUPPLY_HEADER'] = supply_config.get('raw_header')
        self.config['SUPPLY_HEADER_DTYPE'] = supply_config.get('raw_header_dtype')
        self.config['SUPPLY_HIT_HOUR_HEADER'] = supply_config.get('agg_hour_header')
        self.config['SUPPLY_HIT_DAY_HEADER'] = supply_config.get('agg_day_header')
        self.config['SUPPLY_REQS_HOUR_HEADER'] = supply_config.get('reqs_hour_header')
        self.config['SUPPLY_REQS_DAY_HEADER'] = supply_config.get('reqs_day_header')

        #Demand'''
        self.config['DEMAND_HEADER'] = demand_config.get('raw_header')
        self.config['DEMAND_HEADER_DTYPE'] = demand_config.get('raw_header_dtype')
        self.config['DEMAND_AD_HOUR_HEADER'] = demand_config.get('agg_hour_header')
        self.config['DEMAND_AD_DAY_HEADER'] = demand_config.get('agg_day_header')

        self.config['SUPPLY_CSV_FILE_PATH'] = config_file.get('supply_csv_file_path')
        self.config['DEMAND_CSV_FILE_PATH'] = config_file.get('demand_csv_file_path')
        self.config['HOUR_FACTS_FILE_PATH'] = config_file.get('hour_facts_file_path')
        self.config['DAY_FACTS_FILE_PATH'] = config_file.get('day_facts_file_path')
    def init_config(self, config_file):
        '''初始化配置'''
        the_config = {}
        # 读取各种配置配置
        the_config['supply_config'] = config_file.get('supply')
        the_config['demand_config'] = config_file.get('demand')
        the_config['database_config'] = config_file.get('database')
        the_config['table_config'] = config_file.get('db_table')
        the_config['pandas_config'] = config_file.get('pandas')

        # 数据库配置
        the_config['DB_DATABASE'] = the_config['database_config'].get('db_name')
        the_config['DB_USER'] = the_config['database_config'].get('user')
        the_config['DB_PASSWORD'] = the_config['database_config'].get('password')
        the_config['DB_HOST'] = the_config['database_config'].get('host')
        the_config['DB_PORT'] = the_config['database_config'].get('port')

        #Supply
        the_config['SUPPLY_HEADER'] = the_config['supply_config'].get('raw_header')
        the_config['SUPPLY_HEADER_DTYPE'] = the_config['supply_config'].get('raw_header_dtype')
        the_config['SUPPLY_HIT_HOUR_HEADER'] = the_config['supply_config'].get('agg_hour_header')
        the_config['SUPPLY_HIT_DAY_HEADER'] = the_config['supply_config'].get('agg_day_header')
        the_config['SUPPLY_REQS_HOUR_HEADER'] = the_config['supply_config'].get('reqs_hour_header')
        the_config['SUPPLY_REQS_DAY_HEADER'] = the_config['supply_config'].get('reqs_day_header')

        #Demand
        the_config['DEMAND_HEADER'] = the_config['demand_config'].get('raw_header')
        the_config['DEMAND_HEADER_DTYPE'] = the_config['demand_config'].get('raw_header_dtype')
        the_config['DEMAND_AD_HOUR_HEADER'] = the_config['demand_config'].get('agg_hour_header')
        the_config['DEMAND_AD_DAY_HEADER'] = the_config['demand_config'].get('agg_day_header')

        #分隔符
        the_config['INPUT_COLUMN_SEP'] = config_file.get('column_sep')
        the_config['OUTPUT_COLUMN_SEP'] = config_file.get('output_column_sep')

        #每批次处理数据条数(读取文件))
        the_config['READ_CSV_CHUNK'] = the_config['pandas_config'].get('read_csv_chunk')
        #每批次处理数据条数(插入数据库)
        the_config['DB_COMMIT_CHUNK'] = the_config['pandas_config'].get('db_commit_chunk')

        #table names
        the_config['HIT_FACTS_BY_HOUR_TABLE_NAME'] = the_config['pandas_config']\
            .get('hit_facts_by_hour2')
        the_config['REQS_FACTS_BY_HOUR_TABLE_NAME'] = the_config['pandas_config']\
            .get('reqs_facts_by_hour2')
        the_config['AD_FACTS_BY_HOUR_TABLE_NAME'] = the_config['pandas_config']\
            .get('ad_facts_by_hour2')
        the_config['AD_FACTS_BY_DAY_TABLE_NAME'] = the_config['pandas_config']\
            .get('ad_facts_by_day2')
        the_config['HIT_FACTS_BY_DAY_TABLE_NAME'] = the_config['pandas_config']\
            .get('hit_facts_by_day2')
        the_config['REQS_FACTS_BY_DAY_TABLE_NAME'] = the_config['pandas_config']\
            .get('reqs_facts_by_day2')

        # file paths
        the_config['SUPPLY_CSV_FILE_PATH'] = config_file.get('supply_csv_file_path')
        the_config['DEMAND_CSV_FILE_PATH'] = config_file.get('demand_csv_file_path')
        the_config['HOUR_FACTS_FILE_PATH'] = config_file.get('hour_facts_file_path')
        the_config['DAY_FACTS_FILE_PATH'] = config_file.get('day_facts_file_path')

        the_config['PLACEHOLDER'] = -9

        the_config['SUPPLY_REQS_CONDITION_RELATION'] = {
            'total':[]
            }
        the_config['SUPPLY_HIT_CONDITION_RELATION'] = {
                'total':[
                    ('ad_card_id', '!=', -1),
                    ('ad_creative_id', '!=', -1)
                ]
            }
        the_config['DEMAND_CONDITION_RELATION'] = {
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
        self.config = the_config

    def init_params(self, trans_type, start_time):
        '''初始化参数'''
        is_hour = True
        is_day = False
        table_ids = ["date_id"]
        output_root_path = None
        hour_output_root_path = None
        file_suffix = None
        condition_relation = None
        if trans_type.find('hour') != -1 :  # 每小时
            output_root_path = self.config['HOUR_FACTS_FILE_PATH']
            table_ids.append("time_id")
        elif trans_type.find('day') != -1 :  # 每天
            output_root_path = self.config['DAY_FACTS_FILE_PATH']
            hour_output_root_path = self.config['HOUR_FACTS_FILE_PATH']
            is_hour = False
            is_day = True

        month_folder = str(self.start_time.year) + ("%02d" % self.start_time.month)

        if trans_type.find('supply') != -1:# supply
            names = self.config['SUPPLY_HEADER']
            names_dtype = tran_header_dtype(self.config['SUPPLY_HEADER_DTYPE'])
            root_path = self.config['SUPPLY_CSV_FILE_PATH']
            file_suffix = '.product.supply.csv'
            if trans_type.find('hit') != -1:
                condition_relation = self.config['SUPPLY_HIT_CONDITION_RELATION']
            elif trans_type.find('reqs') != -1:
                condition_relation = self.config['SUPPLY_REQS_CONDITION_RELATION']
        elif trans_type.find('demand') != -1:# demand
            names = self.config['DEMAND_HEADER']
            names_dtype = tran_header_dtype(self.config['DEMAND_HEADER_DTYPE'])
            root_path = self.config['DEMAND_CSV_FILE_PATH']
            file_suffix = '.product.demand.csv'
            condition_relation = self.config['DEMAND_CONDITION_RELATION']
        #初始化类型
        self.init_transform_type(trans_type)

        if not os.path.exists(output_root_path + month_folder):
            os.makedirs(output_root_path + month_folder)
        output_file_path = output_root_path + month_folder + \
            os.sep + start_time + "." + self.params['run_type'] + file_suffix

        self.params["is_hour"] = is_hour
        self.params["is_day"] = is_day
        self.params["table_ids"] = table_ids
        self.params["start_time"] = start_time
        self.params["output_root_path"] = output_root_path
        self.params["hour_output_root_path"] = hour_output_root_path
        self.params["month_folder"] = month_folder
        self.params["date"] = int(month_folder + ("%02d" % self.start_time.day))
        self.params["root_path"] = root_path
        self.params["output_file_path"] = output_file_path
        self.params["file_suffix"] = file_suffix
        self.params["names"] = names
        self.params["names_dtype"] = names_dtype
        self.params["condition_relation"] = condition_relation
        self.params["tmp_path"] = output_file_path + ".tmp"
        if is_hour:
            self.params["hour"] = int(start_time.split(".")[1])

        LOG.debug('params init completed : ' + str(self.params))
    def init_transform_type(self, trans_type):
        '''初始化类型'''
        if trans_type == 'supply_hour_hit':
            self.params['group_item'] = self.config['SUPPLY_HIT_HOUR_HEADER']
            self.params['table_name'] = self.config['HIT_FACTS_BY_HOUR_TABLE_NAME']
            self.params['condition_relation'] = self.config['SUPPLY_HIT_CONDITION_RELATION']
            self.params['run_type'] = 'hit'
        elif trans_type == 'supply_hour_reqs':
            self.params['group_item'] = self.config['SUPPLY_REQS_HOUR_HEADER']
            self.params['table_name'] = self.config['REQS_FACTS_BY_HOUR_TABLE_NAME']
            self.params['condition_relation'] = self.config['SUPPLY_REQS_CONDITION_RELATION']
            self.params['run_type'] = 'reqs'
        elif trans_type == 'demand_hour_ad':
            self.params['group_item'] = self.config['DEMAND_AD_HOUR_HEADER']
            self.params['table_name'] = self.config['AD_FACTS_BY_HOUR_TABLE_NAME']
            self.params['run_type'] = 'ad'
        elif trans_type == 'supply_day_hit':
            self.params['group_item'] = self.config['SUPPLY_HIT_DAY_HEADER']
            self.params['table_name'] = self.config['HIT_FACTS_BY_DAY_TABLE_NAME']
            self.params['condition_relation'] = self.config['SUPPLY_HIT_CONDITION_RELATION']
            self.params['run_type'] = 'hit'
        elif trans_type == 'supply_day_reqs':
            self.params['group_item'] = self.config['SUPPLY_REQS_DAY_HEADER']
            self.params['table_name'] = self.config['REQS_FACTS_BY_DAY_TABLE_NAME']
            self.params['condition_relation'] = self.config['SUPPLY_REQS_CONDITION_RELATION']
            self.params['run_type'] = 'reqs'
        elif trans_type == 'demand_day_ad':
            self.params['group_item'] = self.config['DEMAND_AD_DAY_HEADER']
            self.params['table_name'] = self.config['AD_FACTS_BY_DAY_TABLE_NAME']
            self.params['run_type'] = 'ad'
    def load_files(self, start_time_str):
        ''' 加载文件 '''
        if os.path.exists(self.params['tmp_path']):
            LOG.warn("tmp file already exists,remove")
            os.remove(self.params['tmp_path'])
        # 数据集数组，按小时计算只会有一个，按天计算每个文件会产生一个，最多24个数据集
        if self.params['is_hour']:
            return self.load_hour_files(start_time_str)
        elif self.params['is_day']:
            return self.load_day_files()
        return False
    def load_day_files(self):
        '''加载一天的文件'''
        count = 0
        if not self.day_merge:  # 重新计算所有小时数
            parent_path = self.params['root_path'] + self.params['month_folder'] + os.sep
            LOG.info('load dir:' + parent_path)
            file_name_contain_day = str(self.params['date'])
            file_generator = os.walk(parent_path).next()
            if file_generator is not None:
                for file_name in file_generator[2]:
                    file_path = parent_path + file_name
                    if(os.path.isfile(file_path)
                        and file_name.find(file_name_contain_day) != -1
                            and str(file_name).endswith(self.params['file_suffix'])):
                        # 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
                        LOG.info('load file:' + file_path)
                        dataframe = pd.read_csv(file_path, \
                                        sep=self.config['INPUT_COLUMN_SEP'], \
                                        names=self.params['names'], header=None, chunksize=self.\
                                        config['READ_CSV_CHUNK'], index_col=False)
                        LOG.info('handel file:' + file_path)
                        count = count + 1
                        self.transform_section(dataframe)
                    else:
                        LOG.warn("hour file not exists:" + file_path)
                        return False
        else:  # 寻找已经计算过的小时数据合并
            for h24 in range(0, 24):
                hour_file_path = self.params['hour_output_root_path'] + self.params['month_folder']\
                    + os.sep + str(self.params['date']) + (".%02d" % h24) + "."\
                    + self.params['run_type'] + self.params['file_suffix']
                if os.path.isfile(hour_file_path):
                    # 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
                    LOG.info('load file:' + hour_file_path)
                    # coppy group_item 复制复本
                    sum_names = []
                    for item in self.params['group_item']:
                        sum_names.append(item)
                    for key in self.params['condition_relation'].keys():
                        sum_names.append(key)
                    dataframe = pd.read_csv(hour_file_path, sep=self.config['OUTPUT_COLUMN_SEP'],\
                                    names=sum_names, header=None, index_col=False)
                    LOG.info('merge file:' + hour_file_path)
                    dataframe.to_csv(self.params['tmp_path'], sep=self.config['OUTPUT_COLUMN_SEP']\
                                    , header=False, na_rep='0', mode="a")
                    count = count + 1
                else:
                    LOG.warn("merge file not exists:" + hour_file_path)
                    return False
        if count == 0:
            return False
        return True
    def load_hour_files(self, start_time_str):
        '''加载小时文件'''
        file_path = self.params['root_path'] + self.params['month_folder']\
            + os.sep + start_time_str + self.params['file_suffix']
        LOG.info('load file:' + file_path)
        if os.path.exists(file_path):
            # 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
            dataframe = pd.read_csv(file_path, sep=self.config['INPUT_COLUMN_SEP'],\
                            names=self.params['names'],\
                            header=None, chunksize=self.config['READ_CSV_CHUNK'], index_col=False)
            self.transform_section(dataframe)
        else:
            return False
        return True

    # 返回占位数据
    def get_init_data(self, group_item, key):
        '''返回占位数据'''
        obj = {}
        for gitem in group_item:
            if self.params['names_dtype'][gitem] == np.int64:
                obj[gitem] = [self.config['PLACEHOLDER']]
            elif self.params['names_dtype'][gitem] == np.string0:
                obj[gitem] = [str(self.config['PLACEHOLDER'])]
        obj[key] = [0]
        return pd.DataFrame(obj)

    def transform_section(self, data_chunks):
        '''分段转换数据'''
        LOG.info('transform...')
        ###############遍历各个分段，分段数据第一次Group Count后存入临时文件#############

        # for df in self.dfs:
        for chunk in data_chunks:
            grouped = None
            for item in self.params['condition_relation'].items():
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
                    tmp_chunk = self.get_init_data(self.params['group_item'], column_name)
                if grouped is None:
                    grouped = tmp_chunk.groupby(self.params['group_item']).size()
                else:
                    grouped = pd.concat([grouped, tmp_chunk.\
                                        groupby(self.params['group_item']).size()], axis=1)

            # 转换为DataFrame
            groupframe = pd.DataFrame(grouped)
            # 保存到临时CSV文件
            groupframe.to_csv(self.params['tmp_path'], sep=self.config['OUTPUT_COLUMN_SEP'],\
                             header=False, na_rep='0', mode="a")

            # info info
            LOG.info("grouped: " + str(len(chunk)) + " records")

        LOG.info("save to tmp file : " + self.params['tmp_path'])
        ###############遍历各个分段，分段数据第一次Group Count后存入临时文件############
        LOG.info ('transform!')


    def get_del_values(self,sql):
        '''删除SQL传入的参数'''
        del_val = [self.params['date']]
        if self.params['is_hour']:
            del_val.append(self.params["hour"])
            LOG.info ("prepare delete:" + sql + str(self.params['date'])\
                    + "," + str(self.params["hour"]))
        else:
            LOG.info ("prepare delete:" + sql + str(self.params['date']))
        return del_val

    def commit_insert(self,total_groupframe,sql,cur,conn):
        '''执行InsertSql，提交Insert的数据'''
        sql_count = 0
        value_list = []
        for index, row in total_groupframe.iterrows():
            is_tuple = isinstance(index,tuple)#type(index) == tuple
            value = [self.params['date']]
            if self.params['is_hour']:
                value.append(self.params["hour"])
            is_break = False
            for name in self.params['group_item']:
                v_value = None
                if is_tuple:
                    v_value = int(index[self.params['group_item'].index(name)])
                else:
                    v_value = int(index)

                if v_value == self.config['PLACEHOLDER'] or v_value ==\
                    str(self.config['PLACEHOLDER']):  # 表示是占位行
                    is_break = True
                    break

                value.append(v_value)
            if is_break:  # 跳过占位行
                continue
            for r_value in row:  # 这里是计算的值，顺序应当是和condition_relation里的Key对应的
                value.append(int(r_value))

            value_list.append(value)

            sql_count = sql_count + 1

            # 每DB_COMMIT_CHUNK条提交一次
            if sql_count % self.config['DB_COMMIT_CHUNK'] == 0:
                # info info
                LOG.info("commit " + str(sql_count) + "/" + str(len(total_groupframe)) + " records")
                cur.executemany(sql, value_list)
                conn.commit()
                value_list = []

        LOG.info("commit all...")
        if len(value_list) != 0:
            cur.executemany(sql, value_list)
            conn.commit()
        LOG.info("commit all!")

    # 读取CSV文件插入数据库
    def insert(self):
        '''读取CSV文件插入数据库'''
        # coppy group_item 复制复本
        sum_names = []
        for item in self.params['group_item']:
            sum_names.append(item)

        for key in self.params['condition_relation'].keys():
            sum_names.append(key)

        ##############从临时文件读取数据计算最终结果，Group Sum####################
        # info info
        LOG.info("merge the tmp file : " + self.params['tmp_path'])
        # 输出结果到文件
        dateframe = pd.read_csv(self.params['tmp_path'], sep=self.config['OUTPUT_COLUMN_SEP'], \
                         names=sum_names, header=None, dtype=self.params['names_dtype'])
        dateframe.dropna()
        total_grouped = dateframe.groupby(self.params['group_item']).sum()
        ##############从临时文件读取数据计算最终结果，Group Sum####################

        # defub info
        LOG.info("merge result size:" + str(len(total_grouped)))

        total_groupframe = pd.DataFrame(total_grouped)
        LOG.info("write result to csv file:" + self.params['output_file_path'])
        total_groupframe.to_csv(self.params['output_file_path'], sep=\
                self.config['OUTPUT_COLUMN_SEP'], header=False)

        LOG.info("remove tmp file:" + self.params['tmp_path'])
        # 删除临时文件
        os.remove(self.params['tmp_path'])

        # info info
        LOG.info("connect --> db:" + str(self.config['DB_DATABASE']) + ",user:" + \
                str(self.config['DB_USER']) + ",password:***,host:" + str(self.config['DB_HOST'])\
                + ",port:" + str(self.config['DB_PORT']) + "...")
        conn = psy.connect(database=self.config['DB_DATABASE'], user=self.config['DB_USER'], \
                        password=self.config['DB_PASSWORD'], host=self.config['DB_HOST'],\
                        port=self.config['DB_PORT'])
        cur = conn.cursor()

        #################清空表######################
        sql = 'DELETE FROM "' + self.params['table_name'] + '" WHERE '

        insert_column = ''
        value_str = ''

        for tid in self.params['table_ids']:
            if self.params['table_ids'].index(tid) > 0:
                sql = sql + " AND "
                insert_column = insert_column + ","
                value_str = value_str + ","
            sql = sql + " " + tid + " = %s"
            insert_column = insert_column + '"' + tid + '"'
            value_str = value_str + '%s'
        sql = sql + ";"

        cur.execute(sql, self.get_del_values(sql))
        conn.commit()
        #################清空表######################

        #################拼接Insert SQL、组装Insert值######################
        for name in self.params['group_item']:
            insert_column = insert_column + ',"' + str(name) + '"'
            value_str = value_str + ',%s'
        for key in self.params['condition_relation'].keys():
            insert_column = insert_column + ',"' + key + '"'
            value_str = value_str + ',%s'
        sql = 'INSERT INTO "' + self.params['table_name'] + '"(' + \
            insert_column + ') VALUES (' + value_str + ');'
        LOG.info("prepare insert : " + sql)
        #################拼接Insert SQL、组装Insert值######################

        #################分段提交数据######################################
        self.commit_insert(total_groupframe, sql, cur, conn)
        cur.close()
        conn.close()
        #################分段提交数据######################################
        