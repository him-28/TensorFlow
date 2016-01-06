# encoding=utf8
'''
Created on 2015年11月17日

@author: Administrator
'''

import time
import datetime as dt
import os
import yaml

import numpy as np
import pandas as pd

from etl.util.ip_convert import IP_Util
from etl.util.mysqlutil import LoadUtils
from etl.util.mysqlutil import DBUtils
from etl.util import init_log
from etl.logic1.ad_transform_pandas import AdTransformPandas, split_header
from etl.conf.settings import MONITOR_CONFIGS as CNF
from etl.conf.settings import CURRENT_ENV
from etl.conf.settings import FlatConfig as Config

from etl.app import getfilesize
from datetime import datetime

LOG = init_log.init("calculate/logger_platform.conf", 'platformLogger')
ENV_CONF = yaml.load(file("conf/platform_monitor_config.yml"))
SCNF = ENV_CONF[CURRENT_ENV]["store"]

IP_UTIL = IP_Util(ipb_filepath=Config['ipb_filepath'],
            city_filepath=Config['city_filepath'])

class AdPlatFormTranform(AdTransformPandas):
    '''
    !管理后台计算
    '''

    def __init__(self, result_path, data_date):
        '''
        Constructor
        '''
        LOG.info("Welcome to AdPlatFormTranform")
        AdTransformPandas.__init__(self, LOG)
        self.starttime = time.clock()
        self.result_path = result_path
        self.filesize = 0
        self.filename = None
        self.spend_time = 0
        self.tmp_suffix = ".platform.tmp"
        #self.display_poss_tmp_suffix = ".platform.ttmp"
        self.flat_suffix = ".platform.flat"
        self.data_date = data_date

    def calculate(self, input_path, input_filename, alg_file):
        '''计算数据'''

        input_path = input_path.replace("\\", os.sep).replace("/", os.sep)
        flat_input_file_name = self.flat_datas(input_path, input_filename)

        flat_input_file_path = os.path.join(input_path, flat_input_file_name)
        self.filesize = getfilesize(os.path.join(input_path, input_filename))
        self.filename = input_filename

        exec_start_time = dt.datetime.now()  # 开始执行时间
        if not isinstance(alg_file, dict):
            LOG.error("非法参数alg_file：" + str(alg_file))
            return None
        # calculate
        for algorithm in alg_file.iterkeys():
            calcu_result = self.do_calculate(algorithm, alg_file[algorithm], \
                                             input_path, flat_input_file_name)
            if calcu_result == -1:  # 出错
                LOG.error("calculate [" + algorithm + "] failed.")
        exec_end_time = dt.datetime.now()  # 结束执行时间
        exec_takes = exec_end_time - exec_start_time

        LOG.info("all task process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                str(exec_takes.seconds / 60) + " minutes)")
        if os.path.exists(flat_input_file_path):
            os.remove(flat_input_file_path)
        return self.merge_result(alg_file)

    def configure_algorithm(self, trans_type, cnf):
        '''各个维度的配置'''
        algorithm = cnf.get("algorithm")
        trans_type_cnf = algorithm.get(trans_type)
        dtype = split_header(SCNF["raw_header_dtype"])
        self.get("dtype").update(dtype)
        if trans_type_cnf is None:
            LOG.error("can not find algorithm type in config:" + trans_type)
            return False
        self.put("condition", trans_type_cnf.get("condition"))
        group_item = self.__append_addon_item(trans_type_cnf.get("group_item"))
        self.put("group_item", group_item)
        sum_names = []
        for item in group_item:
            sum_names.append(item)
        for key in self.get('condition').keys():
            output_name = self.get('condition')[key]["output_name"]
            sum_names.append(output_name)
            self.get("dtype")[output_name] = np.int64
        self.put("sum_names", sum_names)
        """
        if "display_poss" == trans_type:
            self.put("groupby_list", self.__append_addon_item(['board_id', 'session_id', 'pf', \
                            'server_timestamp', 'ad_event_type', 'request_res', 'tag']))
        """
        return True

    def __append_addon_item(self, old_item):
        '''append addon item'''
        addon_item = SCNF["addon_item"]

        for item in addon_item:
            if item not in old_item:
                if item[0:6] == "query-":
                    continue
                old_item.append(item)
                self.get("dtype")[item] = str
        return old_item

    def merge_result(self, alg_file):
        '''merge results'''
        # alg_file 是无序的，需要保证它每一次从计算到合并文件有序输出
        # 排序Start---------------------------------
        ordered_alg_file = []
        for key, path in alg_file.iteritems():
            ordered_alg_file.append((key, path))
        af_len = len(ordered_alg_file)
        for i in range(0, af_len):
            for j in range(0, af_len):
                key1 = ordered_alg_file[i][0]
                key2 = ordered_alg_file[j][0]
                if key1 > key2:
                    tmp = ordered_alg_file[i]
                    ordered_alg_file[i] = ordered_alg_file[j]
                    ordered_alg_file[j] = tmp
        # 排序End---------------------------------

        dfs = []
        for key, path in ordered_alg_file:
            dataf = pd.read_csv(path, dtype=self.get("dtype"), sep=self.get("output_column_sep"))
            dataf = dataf.rename(columns={'total':key})
            dfs.append(dataf)
        LOG.info("merge result...")
        result_df = pd.concat(dfs, ignore_index=True)
        addon_item = SCNF["result_item"]
        result_df = result_df.groupby(addon_item, as_index=False).sum()
        exec_start_time = dt.datetime.now()  # 开始执行时间
        if not result_df.empty:
            for key in alg_file.keys():
                LOG.info("fill na values in %s file with zero", key)
                result_df[key] = result_df[key].fillna(0)
                result_df[key] = result_df[key].astype(int, na_rep="0")
            LOG.info("write result datas to csv file:%s", self.result_path)
            result_df.to_csv(self.result_path, \
                            dtype=self.get("dtype"), na_rep="0" , index=False, \
                            sep=self.get("output_column_sep"))
            insert_hour(result_df)
            self.spend_time = '%0.2f' % (time.clock() - self.starttime)
        exec_end_time = dt.datetime.now()  # 结束执行时间
        exec_takes = exec_end_time - exec_start_time
        self.log.info("insert_hour process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                      str(exec_takes.seconds / 60) + " minutes)")
        return self.__report_infos(result_df)

    def __report_infos(self, dataframe):
        '''report infos'''
        result_size = 0
        #display_poss = 0
        display_sale = 0
        impression = 0
        impression_end = 0
        click = 0
        if not dataframe.empty:
            result_size = len(dataframe)
            #display_poss = dataframe["display_poss"].sum()
            display_sale = dataframe["display_sale"].sum()
            impression = dataframe["impression"].sum()
            impression_end = dataframe["impression_end"].sum()
            click = dataframe["click"].sum()
        infos = {
             "file_size": self.filesize,
             "file_name": self.filename,
             "result_size": result_size,
             "spend_time": self.spend_time,
             #"display_poss": display_poss,
             "display_sale": display_sale,
             "impression": impression,
             "impression_end": impression_end,
             "click": click
        }
        return infos

    def flat_datas(self, input_path, input_filename):
        '''flat datas'''
        flat_input_filename = input_filename + self.flat_suffix
        flat_input_filepath = os.path.join(input_path, flat_input_filename)
        input_file_path = os.path.join(input_path, input_filename)
        LOG.info("start to flat province id、 city id、server-timestamp:%s", input_file_path)
        trunks = pd.read_csv(input_file_path, \
                             sep=CNF.get('input_column_sep'), chunksize=CNF.get('read_csv_chunk'), \
                        dtype=split_header(CNF.get("header_type")), index_col=False)
        write_mode = 'w'
        error_write_mode = "w"
        for trunk in trunks:
            c_year = self.data_date.year
            c_month = self.data_date.month
            c_day = self.data_date.day
            c_hour = self.data_date.hour

            c_date_timestamp = time.mktime((c_year, c_month, c_day, c_hour, 0, 0, 0, 0, 0))
            LOG.info("fill error timestamp info[%s]", c_date_timestamp)
            trunk["server_timestamp"] = trunk["server_timestamp"].apply(flat_times).astype(float)
            error_trunk = trunk[trunk["server_timestamp"] != c_date_timestamp]
            if not error_trunk.empty:
                err_trunk = len(trunk)
                if err_trunk > 0:
                    error_path = flat_input_filepath + ".err"
                    trunk.to_csv(error_path, sep=CNF.get('input_column_sep'), \
                                 header=(error_write_mode == 'w'), mode=error_write_mode)
                    error_write_mode = "a"
                    LOG.info("write %s error timestamp records to:%s", err_trunk, error_path)
                    trunk = trunk[trunk["server_timestamp"] == c_date_timestamp]
            trunk["year"] = trunk["server_timestamp"].apply(flat_year)
            trunk["month"] = trunk["server_timestamp"].apply(flat_month)
            trunk["day"] = trunk["server_timestamp"].apply(flat_day)
            trunk["hour"] = trunk["server_timestamp"].apply(flat_hour)
            trunk["province_id"] = trunk["ip"].apply(flat_province_id)
            trunk["city_id"] = trunk["ip"].apply(flat_city_id)

            trunk.to_csv(flat_input_filepath, sep=CNF.get('input_column_sep'), \
                         header=(write_mode == 'w'), mode=write_mode)
            if write_mode == 'w':  # 第一次写，覆盖模式、带头
                write_mode = 'a'
        return flat_input_filename

def flat_times(server_timestamp):
    '''flat times'''
    try:
        return str(time.mktime(dt.date.fromtimestamp(float(server_timestamp)).timetuple()))
    except Exception, exc:
        LOG.error("转换server_timestamp出错：%s, %s" , server_timestamp, exc)
        return '-1'
def flat_year(server_timestamp):
    '''flat year'''
    return dt.date.fromtimestamp(float(server_timestamp)).year
def flat_month(server_timestamp):
    '''flat month'''
    return dt.date.fromtimestamp(float(server_timestamp)).month
def flat_day(server_timestamp):
    '''flat day'''
    return dt.date.fromtimestamp(float(server_timestamp)).day
def flat_hour(server_timestamp):
    '''flat hour'''
    return dt.datetime.fromtimestamp(float(server_timestamp)).hour
def flat_province_id(ip_addr):
    '''flat province id'''
    try:
        return IP_UTIL.get_cityInfo_from_ip(ip_addr, 1)
    except Exception, exc:
        LOG.error("转换province_id出错：%s, %s" , ip_addr, exc)
        return '-1'
def flat_city_id(ip_addr):
    '''flat city id'''
    try:
        return IP_UTIL.get_cityInfo_from_ip(ip_addr, 3)
    except Exception, exc:
        LOG.error("转换city_id出错：%s, %s" , ip_addr, exc)
        return '-1'

#from ad_calculate_inventory import DataInsertPool, split_insert_sql
HOUR_INSERT_KEY = "PLATFORM_HOUR_INSERT"
DAY_INSERT_KEY = "PLATFORM_DAY_INSERT"

def insert_data_frames(row_data, db_alias_info, sql_key, db_pool, db_columns):
    '''把数据插入数据库'''
    db_pool.put(sql_key, [row_data[db_alias_info[x]] for x in db_columns])

def split_insert_sql(db_columns, table_name):
    '''拼接 INSERT SQL'''
    first = True
    col_names = ""
    values = ""
    for col in db_columns:
        if first:
            first = False
        else:
            values += ","
            col_names += ","
        values += "%s"
        col_names += col
    sql = "INSERT INTO " + table_name + " (" + col_names + ") VALUES(" + values + ")"
    return sql

class DataInsertPool(object):
    '''插入到数据库的简单池'''
    def __init__(self, conn, commit_size=30000):
        self.commit_size = commit_size
        self.pool = {}
        self.regist_sql_pool = {}
        self.conn = conn
        self.cur = conn.cursor()
        LOG.info("Data Insert Pool inited. pool size : %s" , commit_size)

    def regist_sql(self, key, sql):
        '''注册SQL'''
        self.regist_sql_pool[key] = sql
        LOG.info("regist sql %s: %s", key, sql)

    def put(self, key, value):
        '''加入到池'''
        vlist = self.pool.get(key)
        if not vlist:
            vlist = [value]
            self.pool.update({key:vlist})
        else:
            vlist.append(value)
        v_len = len(vlist)
        if v_len >= self.commit_size:
            sql = self.regist_sql_pool[key]
            LOG.info("start to commit %s records ...", v_len)
            self.cur.executemany(sql, vlist)
            LOG.info("commit completed : %s", sql)
            del vlist
            vlist = []
            self.pool.update({key:vlist})

    def delete(self, tablename, data_date):
        '''删除数据'''
        sql = "delete from %s where year = %s and month = %s and day = %s" % (tablename, data_date.year, data_date.month, data_date.day)
	LOG.info("begin delete mysql: %s", sql)
        self.cur.execute(sql)
        self.conn.commit()

    def flush(self, key):
        '''刷新并提交'''
        if self.pool.has_key(key):
            vlist = self.pool.get(key)
            if vlist:
                sql = self.regist_sql_pool[key]
                self.cur.executemany(sql, vlist)
                self.conn.commit()
                LOG.info("commit %s inserts : %s", len(vlist), sql)
                del vlist
                vlist = []
                self.pool.update({key:vlist})

    def flush_all(self, is_close=False):
        '''刷新并提交'''
        for key in self.pool.keys():
            self.flush(key)
        if is_close:
            self.cur.close()
            self.conn.close()

def insert_hour(datas, table_name):
    '''insert hour values to db'''
    LOG.info("insert hour result to db...")
    try:
        db_alias_info = SCNF["result_item_alias"]
	"""
        if table_name == 'table_pv_name':
            del db_alias_info['slot_id']
            del db_alias_info['mediabuy_id']
            del db_alias_info['creative_id']
            del db_alias_info['display']
            del db_alias_info['impressions']
            del db_alias_info['clicks']
        else:
            del db_alias_info['pv']
	"""
        db_columns = db_alias_info.keys()
        tablename = SCNF[table_name]
        database = SCNF["db_platform"]
        user = SCNF["db_username"]
        passwd = SCNF["db_passwd"]
        host = SCNF["db_host"]
        port = SCNF["db_port"]
        LOG.info("connect %s@%s:%s...", database, host, port)
        conn = DBUtils.get_connection(database, user, passwd, host, port)
        pool = DataInsertPool(conn, commit_size=30000)
        pool.regist_sql(HOUR_INSERT_KEY, \
                        split_insert_sql(db_columns, tablename))
        datas.apply(insert_data_frames, axis=1, db_alias_info=db_alias_info, \
                        sql_key=HOUR_INSERT_KEY, db_pool=pool, db_columns=db_columns)
        pool.flush_all(is_close=True)
        LOG.info("insert job done.")
        LOG.info("insert job done.")
    except Exception, exc:
        LOG.error("insert error: %s", exc)

def insert_day(datas, data_date):
    '''insert day values to db'''
    LOG.info("insert day result to db...")
    try:
        db_alias_info = SCNF["result_item_alias_day"]
        db_columns = db_alias_info.keys()
        tablename = SCNF["day_table_name"]
        database = SCNF["db_platform"]
        user = SCNF["db_username"]
        passwd = SCNF["db_passwd"]
        host = SCNF["db_host"]
        port = SCNF["db_port"]
        LOG.info("connect %s@%s:%s...", database, host, port)
        conn = DBUtils.get_connection(database, user, passwd, host, port)
        pool = DataInsertPool(conn, commit_size=30000)
	try:
	    if data_date.hour > 0:
		pool.delete(tablename, data_date)
	except Exception, exc:
            LOG.error("delete error: %s", exc)
        pool.regist_sql(DAY_INSERT_KEY, \
                        split_insert_sql(db_columns, tablename))
        datas.apply(insert_data_frames, axis=1, db_alias_info=db_alias_info, \
                        sql_key=DAY_INSERT_KEY, db_pool=pool, db_columns=db_columns)
        pool.flush_all(is_close=True)
        LOG.info("insert job done.")
        LOG.info("insert job done.")
    except Exception, exc:
        LOG.error("insert error: %s", exc)
