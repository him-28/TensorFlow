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
from etl.util.mysqlutil import DBUtils
from etl.util import init_log
from etl.logic1.ad_transform_pandas import AdTransformPandas, split_header
from etl.conf.settings import MONITOR_CONFIGS as CNF
from etl.conf.settings import CURRENT_ENV
from etl.conf.settings import FlatConfig as Config
from etl.report.reporter import PF

from etl.app import getfilesize
from datetime import datetime

LOG = init_log.init("util/logger.conf", 'inventoryLogger')
ENV_CONF = yaml.load(file("conf/inventory_monitor_config.yml"))
SCNF = ENV_CONF[CURRENT_ENV]["store"]

IP_UTIL = IP_Util(ipb_filepath=Config['ipb_filepath'],
            city_filepath=Config['city_filepath'])

class AdInventoryTranform(AdTransformPandas):
    '''
    !库存计算
    '''

    def __init__(self, result_path):
        '''
        Constructor
        '''
        LOG.info("Welcome to AdInventoryTranform")
        AdTransformPandas.__init__(self)
        self.starttime = time.clock()
        self.result_path = result_path
        self.filesize = 0
        self.filename = None
        self.spend_time = 0

    def calculate(self, input_path, input_filename, alg_file):
        '''计算数据'''

        input_path = input_path.replace("\\", os.sep).replace("/", os.sep)
        flat_input_file_name = flat_datas(input_path, input_filename)
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
        if "display_poss" == trans_type:
            self.put("groupby_list", self.__append_addon_item(['board_id', 'session_id', 'pf', \
                            'server_timestamp', 'ad_event_type', 'request_res', 'tag']))
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

        # 合并不同的统计指标到同一个文件-------------------Start
        dfs = []
        for key, path in ordered_alg_file:
            dataf = pd.read_csv(path, dtype=self.get("dtype"), sep=self.get("output_column_sep"))
            dataf = dataf.rename(columns={'total':key})
            dfs.append(dataf)
        LOG.info("merge result...")
        result_df = pd.concat(dfs, ignore_index=True)
        addon_item = SCNF["result_item"]
        result_df = result_df.groupby(addon_item, as_index=False).sum()
        result_df["server_timestamp"] = result_df["server_timestamp"].astype(float)
        # 合并不同的统计指标到同一个文件-------------------Start

        # 保存统计结果到硬盘-------------------Start
        if not result_df.empty:
            for key in alg_file.keys():
                LOG.info("fill na values in %s file with zero", key)
                result_df[key] = result_df[key].fillna(0)
                result_df[key] = result_df[key].astype(int, na_rep="0")
            LOG.info("write result datas to csv file:%s", self.result_path)
            result_df.to_csv(self.result_path, \
                            dtype=self.get("dtype"), na_rep="0", index=False, \
                            sep=self.get("output_column_sep"))
            self.spend_time = '%0.2f' % (time.clock() - self.starttime)
        # 保存统计结果到硬盘-------------------End
        return self.__report_infos(result_df)

    def __report_infos(self, dataframe):
        '''report infos'''
        result_size = 0
        display_sale = 0
        display_poss = 0
        if not dataframe.empty:
            result_size = len(dataframe)
            display_sale = dataframe["display_sale"].sum()
            display_poss = dataframe["display_poss"].sum()
        infos = {
             "file_size": self.filesize,
             "file_name": self.filename,
             "result_size": result_size,
             "spend_time": self.spend_time,
             "display_sale": display_sale,
             "display_poss": display_poss
        }
        details = {}
        if not dataframe.empty:
            df2 = dataframe.groupby("pf").sum()
            for the_pf, datas in df2.iterrows():
                details[the_pf] = {
                    "display_sale" : int(datas["display_sale"]),
                    "display_poss" : int(datas["display_poss"])
               }
        infos["details"] = details
        return infos

def flat_datas(input_path, input_filename):
    '''flat datas'''
    flat_input_filename = input_filename + ".flat"
    flat_input_filepath = os.path.join(input_path, flat_input_filename)
    input_file_path = os.path.join(input_path, input_filename)
    LOG.info("start to flat city id、server-timestamp:%s", input_file_path)
    trunks = pd.read_csv(input_file_path, \
                         sep=CNF.get('input_column_sep'), chunksize=CNF.get('read_csv_chunk'), \
                    dtype=split_header(CNF.get("header_type")), index_col=False)
    write_mode = 'w'
    for trunk in trunks:
        trunk["server_timestamp"] = trunk["server_timestamp"].apply(flat_times)
        trunk["city_id"] = trunk["ip"].apply(flat_city_id)
        trunk.to_csv(flat_input_filepath, sep=CNF.get('input_column_sep'), \
                     header=(write_mode == 'w'), mode=write_mode)
        if write_mode == 'w':  # 第一次写，覆盖模式、带头
            write_mode = 'a'
    return flat_input_filename

def flat_times(server_timestamp):
    '''flat times'''
    return str(time.mktime(dt.date.fromtimestamp(float(server_timestamp)).timetuple()))

def flat_city_id(ip_addr):
    '''flat city id'''
    return IP_UTIL.get_cityInfo_from_ip(ip_addr, 3)

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
            self.cur.executemany(sql, vlist)
            self.conn.commit()
            LOG.info("commit %s inserts : %s", v_len, sql)
            del vlist
            vlist = []
            self.pool.update({key:vlist})

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

def insert_data_frames(row_data, db_alias_info, the_pf, db_pool, db_columns):
    '''把数据插入数据库'''
    value_arr = []
    for col in db_columns:
        value = row_data[db_alias_info[col]]
        if col == 'date':
            value = datetime.fromtimestamp(float(value))
        value_arr.append(value)
    db_pool.put(the_pf, value_arr)

def insert(dataframe, commit_size=30000):
    '''insert result to db'''
    LOG.info("insert result to db...")
    try:
        db_alias_info = SCNF["result_item_alias"]
        db_columns = db_alias_info.keys()
        tablename = SCNF["table_name"]
        database = SCNF["db_inventory"]
        user = SCNF["db_username"]
        passwd = SCNF["db_passwd"]
        host = SCNF["db_host"]
        port = SCNF["db_port"]
        LOG.info("connect %s@%s:%s...", database, host, port)
        pool = DataInsertPool(\
                              DBUtils.get_connection(database, user, passwd, host, port), \
                              commit_size)
        dataframe = dataframe.groupby("pf")
        for the_pf, datas in dataframe:
            if PF.has_key(the_pf):
                switch_table_name = tablename + the_pf
                pool.regist_sql(the_pf, \
                                split_insert_sql(db_columns, switch_table_name))
                datas.apply(insert_data_frames, axis=1, db_alias_info=db_alias_info,\
                            the_pf=the_pf, db_pool=pool, db_columns=db_columns)
                pool.flush(the_pf)
            else:
                LOG.error("unknow pf: %s", the_pf)
        pool.flush_all(is_close=True)
        LOG.info("insert job done.")
    except Exception, exc:
        LOG.error("insert error: %s ", exc)
