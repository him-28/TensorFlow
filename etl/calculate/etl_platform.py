# encoding=UTF-8
'''
Created on 2015年12月7日

@author: Dico·Ding
@contact: dingzheng@mgtv.com
'''

import os
import sys
import time
import datetime as dt
import collections

import logging.config
import pandas as pd
import yaml
import numpy as np
import urllib

from etl.util.playerutil import getplayerInfo2
from etl.util.ip_convert import IP_Util
from etl.util.admonitor_flat_data import player_info

from etl.logic1.ad_calculate_platform import insert_hour
from etl.logic1.ad_calculate_platform import insert_day
from datetime import date
from __builtin__ import True
from _collections import defaultdict

CONFIG_ALL = yaml.load(file("calculate/etl_conf_platform_xyda.yml"))

CFG = CONFIG_ALL["platform"]
TAG_CODE = CONFIG_ALL["tag_code"]
AUDIT_CFG = CONFIG_ALL["audit_cfg"]
IP_UTIL = IP_Util(ipb_filepath=CFG['ipb_filepath'],
            city_filepath=CFG['city_filepath'])
def split_header(header):
    '''转换配置里的数据类型、列名'''
    target_dtype = {}
    for name, the_type in header.iteritems():
        if the_type == 'int':
            target_dtype[name] = int
        elif the_type == 'str':
            target_dtype[name] = str
        elif the_type == 'list':
            target_dtype[name] = str
        elif the_type == 'long':
            target_dtype[name] = long
    return target_dtype

def getfilesize(filepath):
    '''filesize = '%0.3f' % (size / 1024.0 / 1024.0)'''
    if not os.path.exists(filepath):
        return 0
    return os.path.getsize(filepath)

def player_info_not_changed(player_infos):
    '''每次只会同步一次播放器和和广告位的对应关系'''
    bslot_dict = {}
    for items in player_infos:
        key = str(items[0])
        value = int(items[1])
        if bslot_dict.has_key(key):
            bslot_dict[key].append(value)
        else:
            bslot_dict[key] = [value]
    return bslot_dict

class ExtractTransformLoadPlatform(object):
    '''
    Extract,Transform and Load tables of data
    '''

    def __init__(self, configure):
        '''
        Constructor
        '''
        self.start_time = time.clock()
        self.end_time = None

        self.params = {}
        self.config_parmas(configure)
        # welcome
        self.welcome()

        self.info("get player infos...")
        self.player_infos = getplayerInfo2()

        self.player_slot_index = {}
        self.player_slot_index = player_info_not_changed(self.player_infos)

    def run(self, run_cfg,merge_cfg):
        '''Run the ETL !!!'''
        result_df = self.extract(run_cfg)  # step 1
        result_df = self.merge_file(merge_cfg)
        #import sys
        #sys.exit()
        if result_df == -1 :
            return -1
        self.save(result_df)  # step 3
        self.end_time = time.clock()
        self.info("all task completed in [%0.2f] seconds", (self.end_time - self.start_time))
        return self.report(result_df)  # step end

    def merge_file(self, merge_cfg):
        result_out_all_file=merge_cfg["result_out_all_file"]
        result_out_file=merge_cfg["result_out_file"]
        result_out_done_file=merge_cfg["result_out_done_file"]
        for done_path in result_out_done_file :
            if not os.path.exists(done_path):
                return -1;


        for out_path in result_out_file :
            if not os.path.exists(out_path):
                return -1;
        dataframe_list = []
        header = self.get("merge_header")
        #print "-------------------*-------------"
        #print header
        #import sys
        #sys.exit()
        for result_path in result_out_file:
            dataframe = pd.read_csv(result_path, engine='c', dtype=self.get("dtype"), \
                            sep=self.get('csv_sep'))
            if dataframe.empty:
                continue
            #如果经过广告位和播放器对应打平后，dataframe为空，此处应该重新做处理
            dataframe_list.append(dataframe)
            self.info("save %s to %s", key, result_path)
        result_df = pd.concat(dataframe_list, ignore_index=True)
        for key in header : 
            result_df[key] = result_df[key].fillna(0).astype(int)
        result_df = result_df.groupby(header, as_index=False, sort=False).sum()
        self.info("merge result to %s", result_out_all_file)
        result_df.to_csv(result_out_all_file, sep=self.get("csv_sep"), index=False)
        return result_df
       
                
    def extract(self, run_cfg, before=None, after=None):
        '''extract data file'''
        if before:
            self.__handle_before(before)
        src_files = self.get("src_files")

        self.regist_alg(run_cfg)

        result_out_file = self.get("result_out_file")
        if os.path.exists(result_out_file):
            os.remove(result_out_file)
            self.warn("output file exists, remove. %s", result_out_file)

        self.set("filesize", 0)

        for file_path in src_files:
            self.set("filesize", self.get("filesize") + getfilesize(file_path))
            if ( file_path.find("yda") == -1):
                self.__extract_file_xda(file_path, run_cfg)
            else:
                self.__extract_file_yda(file_path, run_cfg)
        result_df = self.__merge_chunks_result(run_cfg)

        if after:
            self.__handle_after(after)
        return result_df

    def transform(self, trans_type, alg_file, chunk, before=None, after=None):
        '''transform data'''
        exec_start_time = dt.datetime.now()  # 开始执行时间

        if before:
            self.__handle_before(before)

        '''计算一个维度的数据 '''
        self.info("prepare to calculate : %s", trans_type)
        self.info("according metric to filter data")
        try:
            if trans_type == "display_sale":
                chunk = self.__extract_display_sale_data(chunk, trans_type)
                return self.__caculate_display(chunk, trans_type, alg_file[trans_type])
            else:
                for algorithm in alg_file.iterkeys():
                    if algorithm == "impression":
                        new_chunk = chunk[chunk['event'] == 'impression']
                        calcu_resul = self.__caculate_display(new_chunk, algorithm, alg_file[algorithm])
                        if calcu_resul == -1:
                            self.error("calculate [" + algorithm + "] failed.")                        
                    if algorithm == "click":
                        new_chunk = chunk[chunk['event'] == 'click']
                        calcu_resul = self.__caculate_display(new_chunk, algorithm, alg_file[algorithm])
                        if calcu_resul == -1:
                            self.error("calculate [" + algorithm + "] failed.")
        except Exception, exc:
            self.error("calculate error. %s ", exc)
            self.error("calculate [ %s ] failed.", trans_type)

        if after:
            self.__handle_after(after)
        exec_end_time = dt.datetime.now()  # 结束执行时间
        exec_takes = exec_end_time - exec_start_time

        self.info("process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                str(exec_takes.seconds / 60) + " minutes)")
        
    def regist_alg(self, alg_file):
        '''注册alg信息'''
        info = {}
        for key in alg_file:
            addon = {
                 key:{
                    "header" : CFG["group_item"][key],
                    "write_header" : True
                }
            }
            info.update(addon)
        self.set("alg_info", info)

        self.set("merge_header", CFG["group_item"]["merge"])
        #print "######################"
        #print  CFG["group_item"]["merge"]

    def do_calculate(self, trans_type, output_file_path, chunk):
        '''计算一个维度的数据 '''
        self.info("prepare to calculate : " + trans_type)
        self.info("filter tag < 100 and according metric to filter data")
        try:
            if trans_type == "display_sale":
                #chunk = self.__extract_display_sale_data(chunk, trans_type)
                return self.__caculate_display(chunk, trans_type, output_file_path)
            elif trans_type == "impression":
                return self.__caculate_display(chunk, trans_type, output_file_path)
            elif trans_type == "impression_end":
                return self.__caculate_display(chunk, trans_type, output_file_path)
            elif trans_type == "click":
                return self.__caculate_display(chunk, trans_type, output_file_path)
        except Exception, exc:
            self.error("calculate error. %s ", exc)
            return -1
        return -1

    def __extract_display_sale_data(self, chunk, trans_type):
        '''提取投放数据'''
        self.info("transform data to %s format", trans_type)
        header = self.get("alg_info")[trans_type]["header"]
        series_data_struct = dict((key, []) for key in header)
        chunk.apply(self.__flat_slot_id, axis=1, series_data_struct=series_data_struct)
        self.__dtype_series(series_data_struct)
        new_chunk = pd.DataFrame(series_data_struct)
        return new_chunk

    def __flat_slot_id(self, row_data, series_data_struct):
        '''打平投放SlotID，如果BoardID和SlotID不匹配，不打平'''
        try:
            ad_list = urllib.unquote(row_data["ad_list"])
            board_id = row_data["board_id"]
            if not ad_list:
                return ad_list
            list_array = ad_list.split("col")
            new_row = row_data
            for arr in list_array:
                if not arr or not arr.strip():
                    continue
                try:
                    c="c"
                    aid="aid"
                    mid="mid"
                    cid="cid"                            
                    _d = eval(arr)
                    if _d.has_key("aid") and _d["aid"]:
                        new_row['slot_id'] = _d["aid"]
                    if _d.has_key("mid") and _d["mid"]:
                        new_row['mediabuy_id'] = _d["mid"]
                    if _d.has_key("cid") and _d["cid"]:
                        new_row['creator_id'] = _d["cid"]
                except Exception ,e:
                    self.error("eval adinfo error [%s]"%arr)
                if self.__is_board_slot_id_match(board_id, _d["aid"]):
                    for key, value in series_data_struct.iteritems():
                        value.append(new_row[key])
                        
        except Exception, exc:
            # TODO 记录出错的日志内容
            self.error("flat slot id error: %s\n%s", exc, list(row_data.values))

    def __is_board_slot_id_match(self, board_id, slot_id):
        '''播放器ID和广告位ID是否匹配'''
        slot_id = int(slot_id)
        board_id = board_id
        if slot_id in self.player_slot_index[board_id]:
            return True
        return False

    def __caculate_display(self, chunk, trans_type, output_file_path):
        '''计算展示机会'''
        self.info("caculate %s... " % trans_type)
        header = self.get("alg_info")[trans_type]["header"]
        self.info("init empty data struct with header %s", header)
        write_header = self.get("alg_info")[trans_type]["write_header"]
        if chunk is None or chunk.empty:
            self.info("result is empty. write a empty result file with headers in.")
            if write_header:
                out_file = open(output_file_path, "wb")
                out_file.write(self.get("csv_sep").join(header) + self.get("csv_sep") + trans_type)
                out_file.close()
                self.get("alg_info")[trans_type]["write_header"] = False
            return 0
        result = pd.DataFrame(chunk.groupby(header, as_index=False, sort=False).size())
        if write_header:
            mode = 'w'
        else:
            mode = 'a'
        out_path = os.path.dirname(output_file_path)
        if not os.path.exists(out_path):
            os.makedirs(out_path)
        self.info("save result to %s", output_file_path)
        self.info("the write model is %s", mode)
        result.to_csv(output_file_path, index=True, mode=mode, \
                      header=write_header, sep=self.get("csv_sep"))
        if write_header:
            self.get("alg_info")[trans_type]["write_header"] = False
        return 0

    def __dtype_series(self, series_data_struct):
        '''给Series构造加上数据类型(dtype)'''
        for item, arr in series_data_struct.iteritems():
            series_data_struct[item] = np.array(arr, dtype=self.get("dtype")[item])

    def __fill_relation_list(self, row_data, series_data_struct):
        '''把查询到的需要打平字段的列表分割、组成Series'''
        try:
            board_id = row_data["board_id"]
            timestamp = float(row_data["server_timestamp"])
            if not timestamp:
                timestamp = -1
            slot_ids = self.__get_slot_ids(board_id, timestamp)
            if slot_ids is None:
                self.debug("no slot data with the condition: board_id: %s, timestamp: %s"
                           , board_id, timestamp)
                return row_data
            for slot_id in slot_ids:
                for item, item_data in series_data_struct.iteritems():
                    if item == "slot_id":
                        item_data.append(slot_id)
                    elif item == "server_timestamp":
                        s_ts = time.mktime(dt.datetime.fromtimestamp(timestamp).date().timetuple())
                        item_data.append(s_ts)
                    else:
                        item_data.append(row_data[item])
        except Exception, exc:
            self.error("failed to fill relation: %s\n%s", exc, list(row_data.values))
        return row_data

    def load(self):
        '''加载文件并计算'''
        input_file_path = self.get("result_out_file")
        self.info('load file:' + input_file_path)
        if os.path.exists(input_file_path):
            if os.path.exists(input_file_path):
                # 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
                data_chunks = pd.read_csv(input_file_path, sep=self.get('csv_sep'), \
                                dtype=self.get('dtype'), index_col=False, \
                                chunksize=self.get('chunk'), engine='c')
                return data_chunks
            else:
                with open(self.get('output_file_path'), "wb") as fread:
                    title = self.get("output_column_sep")\
                        .join(s for s in self.get("group_item"))
                    fread.write(title + "\n")
        return None


    def save(self, result_df, before=None, after=None):
        '''save result into DB'''
        if before:
            self.__handle_before(before)
        if after:
            self.__handle_after(after)
        start_time = time.clock()
        result_df['year'] = result_df['year'].astype(int)
        result_df['month'] = result_df['month'].astype(int)
        result_df['day'] = result_df['day'].astype(int)
        result_df['hour'] = result_df['hour'].astype(int)
        insert_hour(result_df)
        end_time = time.clock()
        self.info("insert hour time is: %ss", end_time - start_time)

    def report(self, dataframe, before=None, after=None):
        '''report result to BearyChat,Email'''
        if before:
            self.info("before report: %s", before)
            self.__handle_before(before)

        result_size = 0
        display_sale = 0
        impression = 0
        click = 0
        if not dataframe.empty:
            result_size = len(dataframe)
            display_sale = dataframe["display_sale"].sum()
            impression = dataframe["impression"].sum()
            click = dataframe["click"].sum()
        infos = {
             "file_name": "x.da和y.da",
             "file_size": '%0.3f MB' % (self.get("filesize") / 1024.0 / 1024.0),
             "result_size": result_size,
             "spend_time": "%0.2f" % (self.end_time - self.start_time),
             "display_sale": display_sale,
             "impression": impression,
             "click": click
        }

        if after:
            self.info("after report: %s", after)
            self.__handle_after(after)

        return infos
    def config_parmas(self, configures):
        '''config params'''

        logging.config.fileConfig("calculate/logger_platform.conf")
        if configures.has_key("logger"):
            self.set("logger", configures.logger)
        else:
            self.set("logger", logging.getLogger('platformLogger'))

        src_file_paths = configures.get("src_files")
        if not src_file_paths or not isinstance(src_file_paths, list):
            self.error("Error src file configure: %s", src_file_paths)
            return False
        self.set("src_files", src_file_paths)

        self.set("dtype", split_header(CFG["dtype"]))
        self.set("original_error", src_file_paths[0] + CFG["original_error"])
        self.set("result_out_file", configures["result_out_file"])
        self.set("result_out_done_file", configures["result_out_done_file"])
        self.set("csv_sep", CFG["csv_sep"])

        return True


    def __extract_file_xda(self, input_file_path, run_cfg):
        '''加载文件并计算'''
        self.info("extract file: %s", input_file_path)
        if os.path.exists(input_file_path):
            chunks = pd.read_csv(input_file_path, \
                         chunksize=CFG["read_csv_chunksize"], \
                         engine='c', sep="\n")
            keys = CFG["usefull_request_body_header_x"].keys()
            values = CFG["usefull_request_body_header_x"].values()                               
            for chunk in chunks:
                self.info("handle %s records...", len(chunk))
                new_chunk = pd.DataFrame()

                # self.set("chunk_idx", -1)
                req_cols = collections.OrderedDict(\
                                    (x, []) for x in values)

                tag_list = []
                # 打平、转换、审计,用于展示机会统计
                chunk.apply(self.__split_request_body_xda, axis=1, \
                                req_cols=req_cols, keys=keys, values=values, tag_list=tag_list)

                for name, datas in req_cols.iteritems():
                    new_chunk[name] = datas

                trans_type = "display_sale"
                self.transform(trans_type, run_cfg, new_chunk)
        else:
            self.error("failed to extract file :%s, file not exists.", input_file_path)
            return False
        return True
    
    def __extract_file_yda(self, input_file_path, run_cfg):
        '''加载文件并计算'''
        self.info("extract file: %s", input_file_path)
        if os.path.exists(input_file_path):
            chunks = pd.read_csv(input_file_path, \
                         chunksize=CFG["read_csv_chunksize"], \
                         engine='c', sep="\n")
            keys = CFG["usefull_request_body_header_y"].keys()
            values = CFG["usefull_request_body_header_y"].values()                         
            for chunk in chunks:
                self.info("handle %s records...", len(chunk))
                new_chunk = pd.DataFrame()

                # self.set("chunk_idx", -1)
                req_cols = collections.OrderedDict(\
                                    (x, []) for x in values)

                tag_list = []
                # 打平、转换、审计,用于展示机会统计
                chunk.apply(self.__split_request_body_yda, axis=1, \
                                req_cols=req_cols, keys=keys, values=values, tag_list=tag_list)

                for name, datas in req_cols.iteritems():
                    new_chunk[name] = datas
                trans_type = "y.da"
                self.transform(trans_type, run_cfg, new_chunk)
        else:
            self.error("failed to extract file :%s, file not exists.", input_file_path)
            return False
        return True
    
    def __merge_chunks_result(self, run_cfg):
        '''merge'''
        self.info("merge chunk files...")
        dataframe_list = []
        tag_key = "xx"
        for key, result_path in run_cfg.iteritems():
            header = self.get("alg_info")[key]["header"]
            dataframe = pd.read_csv(result_path, engine='c', dtype=self.get("dtype"), \
                            sep=self.get('csv_sep'))
            if dataframe.empty:
                tag_key = key
                continue
            for h in header:
                dataframe[h] = dataframe[h].fillna(-1)
            dataframe = dataframe.groupby(header, as_index=False, sort=False).sum().rename(columns={'0':key})
            #如果经过广告位和播放器对应打平后，dataframe为空，此处应该重新做处理
            dataframe_list.append(dataframe)
            self.info("save %s to %s", key, result_path)
            dataframe.to_csv(result_path, index=False, \
                             header=True, sep=self.get("csv_sep")) 
        result_df = pd.concat(dataframe_list, ignore_index=True)
        for key in run_cfg.keys():
            if tag_key == key:
                result_df[key] = 0
                continue
            result_df[key] = result_df[key].fillna(0).astype(int)
        result_df = result_df.groupby(header, as_index=False, sort=False).sum()
        result_out_file = self.get("result_out_file")
        result_out_done_file = self.get("result_out_done_file")
        self.info("merge result to %s", result_out_file)
        with open(result_out_done_file,"wb") as create_file:
            pass

        result_df.to_csv(result_out_file, sep=self.get("csv_sep"), index=False)
        return result_df

    def __split_request_body_xda(self, row_datas, req_cols, keys, values, tag_list):
        '''拆分reqeust body'''
        # self.set("chunk_idx", self.get("chunk_idx") + 1)
        # 打平--------------------------------------Start
        seri = dict((x, "") for x in values)
        try:                    
            row_data_all = row_datas[0].split(CFG["original_sep_x"])
            # 检查时间有效性
            row_data_date = row_data_all[0].split(" ")
            seri["year"] = row_data_date[0][0:4]
            seri["month"] = row_data_date[0][5:7]
            seri["day"] = row_data_date[0][8:10]
            seri["hour"] = row_data_date[1][0:2]
            # 检查其它字段
            row_data = row_data_all[1].split(" ")
            for kvalues in row_data:
                if not kvalues or not kvalues.strip():
                    continue
                kv_row = kvalues.split("=")
                if not kv_row or len(kv_row) != 2:
                    continue
                if kv_row[0] == 'adinfo':
                    seri["ad_list"] = seri["ad_list"] +  'col' + kv_row[1]
                    """
                    key_name = "ad_list" + str(adlen_count)
                    seri[key_name] = kv_row[1]
                    """
                if kv_row[0] == "ip":
                    if kv_row[1]:
                        try:
                            seri["province_id"] = IP_UTIL.get_cityInfo_from_ip(kv_row[1], 1)
                            seri["city_id"] = IP_UTIL.get_cityInfo_from_ip(kv_row[1], 3)
                        except Exception, exc:
                            seri["province_id"] = -1
                            seri["city_id"] = -1
                    else:
                        seri["province_id"] = -1
                        seri["city_id"] = -1
                if kv_row[0] == 'pid':
                    seri["board_id"] = kv_row[1]
                
        except Exception, exc:
            self.error("can not split request_body:%s\n%s", exc, row_data)
            # TODO 记录出错的日志内容
            return row_datas
        # 打平--------------------------------------End

        # 审计--------------------------------------Start
        #tag = self.__audit_orign_rows(seri, tag_list)
        # 审计--------------------------------------End
        #seri["tag"] = tag
        for key, value in req_cols.iteritems():
            value.append(seri[key])

        return row_datas
    
    def __split_request_body_yda(self, row_datas, req_cols, keys, values, tag_list):
        '''拆分reqeust body'''
        # self.set("chunk_idx", self.get("chunk_idx") + 1)
        # 打平--------------------------------------Start
        seri = dict((x, "") for x in values)
        try:                    
            row_data_all = row_datas[0].split(CFG["original_sep_y"])
            # 检查时间有效性
            if len(row_data_all) != 14:
                return row_datas
            seri["day"] = row_data_all[1][0:2]
            #seri["month"] = row_data_all[1][3:6]
            seri["month"] = '12'
            seri["year"] = row_data_all[1][7:11]
            seri["hour"] = row_data_all[1][12:14]
            if row_data_all[0]:
                try:
                    seri["province_id"] = IP_UTIL.get_cityInfo_from_ip(row_data_all[0], 1)
                    seri["city_id"] = IP_UTIL.get_cityInfo_from_ip(row_data_all[0], 3)
                except Exception, exc:
                    seri["province_id"] = -1
                    seri["city_id"] = -1
            else:
                seri["province_id"] = -1
                seri["city_id"] = -1
            row_data = row_data_all[3].split("&")
            for kvalues in row_data:
                kv_row = kvalues.split("=")
                if kv_row[0].find("impression") != -1:
                    seri["event"] = "impression"
                elif kv_row[0].find("click") != -1:
                    seri["event"] = "click"
                if kv_row[0] == 'b':
                    seri["board_id"] = kv_row[1]
                if kv_row[0] == 's':
                    seri["slot_id"] = kv_row[1]
                if kv_row[0] == 'cd':
                    seri["mediabuy_id"] = kv_row[1]
                if kv_row[0] == 'ct':
                    seri["creator_id"] = kv_row[1]
                if kv_row[0] == 'o':
                    seri["order"] = kv_row[1]
        except Exception, exc:
            self.error("can not split request_body:%s\n%s", exc, row_data)
            # TODO 记录出错的日志内容
            return row_datas
        # 打平--------------------------------------End

        # 审计--------------------------------------Start
        #tag = self.__audit_orign_rows(seri, tag_list)
        # 审计--------------------------------------End
        #seri["tag"] = tag
        for key, value in req_cols.iteritems():
            value.append(seri[key])
        return row_datas
        
    def __flat_datas(self, values, items, http_x_forwarded_for, remote_addr, time_iso8601):
        '''打平IP、CityID，server_timestamp'''
        seri = dict((x, "") for x in values)
        for item in items:
            k_v = item.split("=")
            usefull_header = CFG["usefull_request_body_header"]
            if usefull_header.has_key(k_v[0]):
                key = usefull_header[k_v[0]]
                seri[key] = k_v[1]

        # 拆分IP
        if isinstance(http_x_forwarded_for, str):
            if http_x_forwarded_for.strip() == "-":
                seri["ip"] = remote_addr
            else:  # 代理第一跳
                seri["ip"] = http_x_forwarded_for.strip().split(",")[0]
        else:
            seri["ip"] = remote_addr

        try:
            seri["province_id"] = IP_UTIL.get_cityInfo_from_ip(seri["ip"], 1)
            seri["city_id"] = IP_UTIL.get_cityInfo_from_ip(seri["ip"], 3)
        except Exception, exc:
            seri["province_id"] = -1
            seri["city_id"] = -1
        #必须在小时数之前转化linux时间戳
        server_timestamp = time_iso8601  # [2015-12-04T14:00:02+08:00]
        d_date = dt.datetime.strptime(server_timestamp, '[%Y-%m-%dT%H:%M:%S+08:00]') #%datetime类型
        d_tuple = d_date.timetuple() #转化为time tuple
        seri["server_timestamp"] = time.mktime(d_tuple) #转化linux时间戳

        d_date = dt.datetime(d_date.year,d_date.month,d_date.day,d_date.hour,00) #%只取小时数
        seri["year"] = d_date.year
        seri["month"] = d_date.month
        seri["day"] = d_date.day
        seri["hour"] = d_date.hour

        return seri

    def __audit_orign_rows(self, row_data, tag_list):
        '''审核每一行数据'''
        try:
            tag = TAG_CODE["AUDIT_CORRECT"]
            for name, rules in AUDIT_CFG.iteritems():
                if not name in row_data:
                    continue
                value = row_data[name]
                is_break = False
                for scene_rules in rules.values():

                    condition = scene_rules.get("condition")
                    if condition:
                        should_vali = True
                        for c_key, c_value in condition.iteritems():
                            if not row_data[c_key] in c_value:
                                should_vali = False
                        if should_vali:
                            required = scene_rules.get("scene_rules")
                            value_range = scene_rules.get("range")
                            dtype = scene_rules.get("dtype")
                            if not self.__validate(value, required, value_range, dtype):
                                tag = TAG_CODE["AUDIT_ERROR"]
                                is_break = True
                                break
                    else:
                        required = scene_rules.get("scene_rules")
                        value_range = scene_rules.get("range")
                        dtype = scene_rules.get("dtype")
                        if not self.__validate(value, required, value_range, dtype):
                            tag = TAG_CODE["AUDIT_ERROR"]
                            is_break = True
                            break
                if is_break:
                    break
        except Exception, exc:
            self.error("audit error:%s\n%s", exc, list(row_data.values))
            tag = TAG_CODE["AUDIT_EXCEPT"]
        return tag

    def __validate(self, value, required, value_range, dtype):
        '''验证'''
        if required and not value:
            return False
        if value and value_range:
            if not value in value_range:
                return False
        if value and dtype:
            if dtype == 'float':
                try:
                    float(value)
                except:
                    return False
            if dtype == 'int':
                try:
                    int(value)
                except:
                    return False
        return True

    def __get_slot_ids(self, board_id, timestamp):
        '''查询board_id下对应的slot_id'''
        for group in self.player_infos.values():
            start = group['starttime']
            end = group['endtime']
            if start <= timestamp and end > timestamp:
                playerinfo = group["playerinfo"]
                if playerinfo.has_key(int(board_id)):
                    return playerinfo[int(board_id)].keys()
        return None

    """
    def __get_player_infos(self):
        '''获取播放器信息，如果已经获取过，从缓存中拿'''
        if self.player_id_cache is None:
            self.player_id_cache = getplayerInfo()
        return self.player_id_cache
    """

    def __handle_before(self, before):
        '''apply before from_fun'''
        from_fun = sys._getframe().f_code.co_name
        if isinstance(before, dict):
            self.info("before %s: %s", from_fun, before["fun"].func_name)
            apply(before["fun"], before["params"])
        else:
            self.info("before %s: %s", from_fun, before.func_name)
            apply(before)

    def __handle_after(self, after):
        '''apply after from_fun'''
        from_fun = sys._getframe().f_code.co_name
        if isinstance(after, dict):
            self.info("before %s: %s", from_fun, after["fun"].func_name)
            apply(after["fun"], after["params"])
        else:
            self.info("before %s: %s", from_fun, after.func_name)
            apply(after)

    def get(self, key):
        '''get params'''
        return self.params.get(key)

    def set(self, key, value):
        '''set params value'''
        self.params[key] = value

    def info(self, msg, *param):
        '''logger info'''
        self.get("logger").info(msg, *param)

    def error(self, msg, *param):
        '''logger err'''
        self.get("logger").error(msg, *param)

    def warn(self, msg, *param):
        '''logger warn'''
        self.get("logger").warn(msg, *param)

    def debug(self, msg, *param):
        '''logger debug'''
        self.get("logger").debug(msg, *param)

    def welcome(self):
        '''welcome!'''
        for bless in self.blesses():
            print bless

    def blesses(self):
        '''佛祖保佑'''
        b_b_m = []
        b_b_m.append(r'''      #####################################################''')
        b_b_m.append(r'''     ####################################################### ''')
        b_b_m.append(r'''    #########################################################''')
        b_b_m.append(r'''    ##                                                     ##''')
        b_b_m.append(r'''    ##                       _oo0oo_                       ##''')
        b_b_m.append(r'''    ##                      o8888888o                      ##''')
        b_b_m.append(r'''    ##                      88" . "88                      ##''')
        b_b_m.append(r'''    ##                      (| -_- |)                      ##''')
        b_b_m.append(r'''    ##                      0\  =  /0                      ##''')
        b_b_m.append(r'''    ##                   ___//`---'\\___                   ##''')
        b_b_m.append(r'''    ##                  .' \|       |/ '.                  ##''')
        b_b_m.append(r'''    ##                 /  \|||  :  |||/  \                 ##''')
        b_b_m.append(r'''    ##                / _||||| -:- |||||- \                ##''')
        b_b_m.append(r'''    ##               /  _||||| -:- |||||-  \               ##''')
        b_b_m.append(r'''    ##               | \_|  ''\---/''  |_/ |               ##''')
        b_b_m.append(r'''    ##               \  .-\__  '-'  ___/-. /               ##''')
        b_b_m.append(r'''    ##             ___'. .'  /--.--\  `. .'___             ##''')
        b_b_m.append(r'''    ##          ."" '<  `.___\_<|>_/___.' >' "".           ##''')
        b_b_m.append(r'''    ##         | | :  `- \`.;`\ _ /`;.`/ - ` : | |         ##''')
        b_b_m.append(r'''    ##         \  \ `_.   \_ __\ /__ _/   .-` / /          ##''')
        b_b_m.append(r'''    ##                       `=---='                       ##''')
        b_b_m.append(r'''    ##         Welcome to ExtractTransformLoad !           ##''')
        b_b_m.append(r'''    ##     Amituofo, buddha bless me, never have bug       ##''')
        b_b_m.append(r'''    ####@#######@#######@#######@#####@#####@#######@######@#''')
        b_b_m.append(r'''  #@##@##@####@##@####@##@####@##@##@##@##@##@####@##@###@##@##''')
        b_b_m.append(r'''#####@####@#@#####@#@#####@#@#####@#####@#####@#@#####@#@####@###''')
        self.debug("南无阿弥陀佛，我佛慈悲为怀，无BUG造福众生万物！")
        return b_b_m



class FilterWriterPool(object):
    '''错误日志存档'''
    def __init__(self, file_path, chunksize=10000):
        '''构造方法'''
        self.chunksize = chunksize
        self.cache = []
        self.input_file = open(file_path, "wb")

    def put(self, row):
        '''加入缓存'''
        self.cache.append(CFG["original_sep"].join(row))
        if len(self.cache) > self.chunksize:
            for row in self.cache:
                self.input_file.write(row)
            del self.cache
            self.cache = []

    def flush(self):
        '''刷新、关闭'''
        if len(self.cache) > 0:
            for row in self.cache:
                self.input_file.write(row)
        self.input_file.close()
