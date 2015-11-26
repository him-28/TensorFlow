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
from etl.logic1.ad_transform_pandas import AdTransformPandas
from etl.conf.settings import MONITOR_CONFIGS as CNF
from etl.conf.settings import CURRENT_ENV
from etl.conf.settings import FlatConfig as Config
from etl.conf.settings import HEADER

from etl.app import getfilesize

LOG = init_log.init("util/logger.conf", 'inventoryLogger')
ENV_CONF = yaml.load(file("conf/inventory_monitor_config.yml"))
SCNF = ENV_CONF[CURRENT_ENV]["store"]

class AdInventoryTranform(AdTransformPandas):
    '''
    !库存计算
    '''

    def __init__(self, result_path):
        '''
        Constructor
        '''
        LOG.info("Welcome to AdInventoryTranform")
        self.starttime = time.clock()
        self.ip_util = IP_Util(ipb_filepath=Config['ipb_filepath'],
                    city_filepath=Config['city_filepath'])
        self.params = {}
        self.player_id_cache = None
        self.result_path = result_path
        self.filesize = 0
        self.filename = None
        self.spend_time = 0

    def calculate(self, input_path, input_filename, alg_file):
        '''计算数据'''

        input_path = input_path.replace("\\", os.sep).replace("/", os.sep)
        flat_input_file_name = self.__flat_times(input_path, input_filename)
        flat_input_file_path = os.path.join(input_path, flat_input_file_name)
        self.filesize = getfilesize(flat_input_file_path)
        self.filename = input_filename

        exec_start_time = dt.datetime.now()  # 开始执行时间
        if not isinstance(alg_file, dict):
            LOG.error("非法参数alg_file：" + str(alg_file))
            return None
        # handle path
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
        if not result_df.empty:
            for key in alg_file.keys():
                LOG.info("fill na values in %s file with zero", key)
                result_df[key] = result_df[key].fillna(0)
                result_df[key] = result_df[key].astype(int, na_rep="0")
            LOG.info("write result datas to csv file:%s", self.result_path)
            result_df.to_csv(self.result_path, \
                            dtype=self.get("dtype"), na_rep="0" , index=False, sep=self.get("output_column_sep"))
            self.spend_time = '%0.2f' % (time.clock() - self.starttime)
        return self.__report_infos(result_df)

    def __report_infos(self, dataframe):
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
            for pf,datas in df2.iterrows():
                details[pf] = {
                    "display_sale" : int(datas["display_sale"]),
                    "display_poss" : int(datas["display_poss"])
               }
        infos["details"] = details
        return infos

    def __flat_times(self, input_path, input_filename):
        '''按时间打平'''
        # TODO 加入批量读写
        # 分隔符
        input_column_sep = CNF.get("input_column_sep")
        # 定位tag位置
        tag_index = HEADER.index("tag")
        # 定位IP位置
        ip_index = HEADER.index("ip")
        server_timestamp_index = HEADER.index("server_timestamp")
        column_len = len(HEADER)
        filepath = os.path.join(input_path, input_filename)
        LOG.info("start to flat city id:%s",filepath)
        input_filename = input_filename + ".flat"
        flat_file_path = os.path.join(input_path, input_filename)
        flat_file = open(flat_file_path, "wb")
        first_row = True
        with open(filepath, "r+") as fileline:
            for line in fileline:
                line = line.replace("\n", "")
                if first_row:  # 标题行处理
                    first_row = False
                    flat_file.write(line)
                    flat_file.write(input_column_sep)
                    flat_file.write("city_id")
                    flat_file.write("\n")
                    continue

                line_list = line.split(input_column_sep)
                if not len(line_list) == column_len:
                    continue  # error line
                tag = line_list[tag_index]
                if (not tag) or (int(tag) > 99):
                    continue  # error line
                server_timestamp = line_list[server_timestamp_index]
                s_date = dt.date.fromtimestamp(float(server_timestamp)) # 精确到天
                line_list[server_timestamp_index] = str(time.mktime(s_date.timetuple()))
                ip_addr = line_list[ip_index]
                city_id = self.ip_util.get_cityInfo_from_ip(ip_addr, 3)
                line_list.append(str(city_id))
                is_first = True
                for item in line_list:
                    if is_first:
                        is_first = False
                    else:
                        flat_file.write(input_column_sep)
                    flat_file.write(item)
                flat_file.write("\n")
        flat_file.close()
        LOG.info("flat completed, result saved at %s", input_filename)
        return input_filename


def insert(result_path):
    '''insert csv values to db'''
    LOG.info("insert csv file to db,csv file:%s", result_path)
    try:
        db_columns = SCNF["result_item_alias"]
        tablename = SCNF["table_name"]
        database = SCNF["db_inventory"]
        user = SCNF["db_username"]
        passwd = SCNF["db_passwd"]
        host = SCNF["db_host"]
        port = SCNF["db_port"]
        LOG.info("connect %s@%s:%s...", database, host, port)
        conn = DBUtils.get_connection(database, user, passwd, host, port)
        sep = SCNF["split_char"]
        LoadUtils.fromCsvtodb(result_path, tablename, conn, \
                              cols=db_columns, commit=False, split_char=sep, \
                              skip_first_row=True, cols_type={"date":"timestamp"})
        LOG.info("insert job done.")
    except Exception, exc:
        LOG.error("insert error,csv file: %s\n,%s", result_path, exc)
