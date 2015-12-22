# encoding=UTF-8
'''
Created on 2015年12月7日

@author: Dico·Ding
@contact: dingzheng@mgtv.com
'''

import os
import yaml
import time
import datetime as dt
import collections

import logging.config
import pandas as pd
import numpy as np
import urllib

from etl.util.playerutil import getplayerInfo
from etl.util.mysqlutil import DBUtils
from etl.calculate.etl_inventory import split_header, \
getfilesize, TAG_CODE, AUDIT_CFG, IP_UTIL

CONFIG_ALL = yaml.load(file("calculate/etl_time_conf.yml"))

CFG = CONFIG_ALL["inventory"]

# 正一位广告位
BOARD_CENTER_ONE = CFG["board_center_one"]

from etl.util import playerutil

def get_middle_slot_ids():
    '''获取所有中贴片'''
    database = CFG["db_inventory"]
    user = CFG["db_username"]
    passwd = CFG["db_passwd"]
    host = CFG["db_host"]
    port = CFG["db_port"]
    conn = DBUtils.get_connection(database, user, passwd, host, port)
    cur = conn.cursor()
    sql = "SELECT player_id,adspace_id FROM adspace_type WHERE \
adspace_type_id=2 AND player_id <> -1"
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    result = {}
    for row in rows:
        board_id = int(row[0])
        slot_id = int(row[1])
        if result.has_key(board_id):
            result[board_id].append(slot_id)
        else:
            result[board_id] = [slot_id]
    return result

def get_all_pause_start():
    '''获取所有的暂停、开机大图Slot'''
    database = CFG["db_inventory"]
    user = CFG["db_username"]
    passwd = CFG["db_passwd"]
    host = CFG["db_host"]
    port = CFG["db_port"]
    conn = DBUtils.get_connection(database, user, passwd, host, port)
    cur = conn.cursor()
    sql = "SELECT adspace_id FROM adspace_type WHERE \
(adspace_type_id=4 OR adspace_type_id=7) AND player_id <> -1"
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def get_video_durations():
    '''获取所有视频的时长信息'''
    database = CFG["db_cms"]
    user = CFG["cms_username"]
    passwd = CFG["cms_passwd"]
    host = CFG["cms_host"]
    port = CFG["cms_port"]
    conn = DBUtils.get_connection(database, user, passwd, host, port)
    sql = "SELECT id, duration FROM hunantv_v_videos ORDER BY id ASC"
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

class ExtractTransformLoadTimeInventory(object):
    '''
    Extract,Transform and Load tables of data
    '''

    def __init__(self, configure):
        '''
        Constructor
        '''
        start = time.clock()
        self.params = {}
        self.config_parmas(configure)
        # welcome
        self.welcome()

        self.set("start_time", start)
        self.set("end_time", None)
        self.info("get player infos...")
        self.player_infos = getplayerInfo()

        # self.player_index = player_info_not_changed(\
        #            configure["start_time"], configure["end_time"], self.player_infos)
        # self.info("get player info change status: %s", self.player_index)

        # self.player_slot_index = {}
        # if self.player_index:
        #    for board_id, slots in self.player_index.iteritems():
        #        self.player_slot_index.update({board_id: slots.keys()})

        self.middle_slot_ids = get_middle_slot_ids()
        self.video_duration = {}
        self.p_s_slot_ids = get_all_pause_start()
        self.player_info = playerutil.getplayerInfo2()
        self.board_slot_id_df = self.get_newest_slot_ids(self.player_info)
        self.info("params configed as : %s", self.params)

    def get_newest_slot_ids(self, player_info):
        '''最新的播放器ID、slot_id'''
        info = [(x, y) for x, y in player_info]
        return pd.DataFrame(info, columns=['board_id', 'slot_id'], dtype=int)

    def run(self, run_cfg):
        '''Run the ETL !!!'''
        self.info("start to run with params: \n %s", run_cfg)
        self.extract(run_cfg)  # step 1
        self.set("end_time", time.clock())
        self.info("all task completed in [%0.2f] seconds", \
                  (self.get("end_time") - self.get("start_time")))
        # return self.report(result_df)  # step end

    def extract(self, run_cfg):
        '''extract data file'''
        src_files = self.get("src_files")

        if run_cfg.has_key("pv2"):
            v_d_row = get_video_durations()
            for video, duration in v_d_row:
                self.video_duration[int(video)] = int(duration)

        result_out_file = self.get("result_out_file")
        if os.path.exists(result_out_file):
            os.remove(result_out_file)
            self.warn("output file exists, remove. %s", result_out_file)

        self.set("filesize", 0)

        self.regist_alg(run_cfg)

        for file_path in src_files:
            self.set("filesize", self.get("filesize") + getfilesize(file_path))
            self.__extract_file(file_path, run_cfg)
        # result_df = self.__merge_chunks_result(run_cfg)

        # return result_df

    def transform(self, alg_file, chunk):
        '''transform data'''
        exec_start_time = dt.datetime.now()  # 开始执行时间

        for algorithm in alg_file.iterkeys():
            calcu_result = self.do_calculate(algorithm, alg_file[algorithm], \
                                             chunk)
            if calcu_result == -1:  # 出错
                self.error("calculate [" + algorithm + "] failed.")

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

    def do_calculate(self, trans_type, output_file_path, chunk):
        '''计算一个维度的数据 '''
        self.info("prepare to calculate : " + trans_type)
        try:
            if trans_type == "display_sale":
                chunk = self.__extract_display_sale_data(chunk, trans_type)
                return self.__caculate_display(chunk, trans_type, output_file_path)
            elif trans_type == "pv1":
                # chunk = self.__extract_pv1_data(chunk, trans_type)
                return self.__caculate_display(chunk, trans_type, \
                                        output_file_path)
            elif trans_type == "pv2":
                chunk = self.__extract_pv2_data(chunk, trans_type)
                return self.__caculate_display(chunk, trans_type, \
                                output_file_path, is_sum=True, sum_key="duration")
        except Exception, exc:
            self.error("calculate error. %s ", exc)
            return -1
        return -1

    def __extract_pv1_data(self, chunk, trans_type):
        '''提取投放数据'''
        self.info("提取、打平数据库广告位ID")
        header = self.get("alg_info")[trans_type]["header"]
        series_data_struct = dict((key, []) for key in header)
        chunk.apply(self.__fill_pv1_slot_list, axis=1, series_data_struct=series_data_struct)
        self.__dtype_series(series_data_struct)
        new_chunk = pd.DataFrame(series_data_struct)
        return new_chunk

    def __extract_pv2_data(self, chunk, trans_type):
        '''提取投放数据'''
        self.info("提取、打平数据库广告位ID")
        header = self.get("alg_info")[trans_type]["header"]
        series_data_struct = dict((key, []) for key in header)
        series_data_struct["duration"] = []
        series_data_struct["type_id"] = []
        chunk.apply(self.__fill_pv2_slot_list, axis=1, series_data_struct=series_data_struct)
        if not series_data_struct.values()[0]:
            return None
        self.__dtype_series(series_data_struct)
        new_chunk = pd.DataFrame(series_data_struct)
        return new_chunk

    def get_video_duration(self, video_id, rows):
        '''获取视频时长'''
        end = len(rows)
        middle = int(float(end) / 2.0)
        if middle == end:
            return None
        s_video_id = rows[middle][0]
        if s_video_id == video_id:
            return rows[middle][1]
        elif s_video_id < video_id:
            return self.get_video_duration(video_id, rows[middle + 1:])
        elif s_video_id > video_id:
            return self.get_video_duration(video_id, rows[:middle])
        elif end == middle:
            return None

    def trans_duration_2_pv(self, video_id):
        '''把视频时长转换成PV'''
        if not video_id:
            return None
        # duration = self.get_video_duration(video_id, self.video_duration)
        duration = self.video_duration[video_id]
        if duration < 301:  # 大于0分钟小于等于5分钟
            duration = 1
        elif duration > 300 and duration < 601:  # 大于5分钟小于等于10分钟
            duration = 2
        elif duration > 600 and duration < 1201:  # 大于10分钟小于等于20分钟
            duration = 3
        elif duration > 1200:
            duration = 4
        return duration

    def __fill_pv1_slot_list(self, row_data, series_data_struct):
        '''把查询到的需要打平字段的列表分割、组成Series'''
        try:
            board_id = row_data["board_id"]
            slot_ids = self.__get_pv1_slot_ids(board_id)
            if slot_ids is None:
                return row_data
            for slot_id in slot_ids:
                for item, item_data in series_data_struct.iteritems():
                    if item == "slot_id":
                        item_data.append(slot_id)
                    else:
                        item_data.append(row_data[item])
        except Exception, exc:
            self.error("failed to fill relation: %s\n%s", exc, list(row_data.values))
        return row_data

    def __fill_pv2_slot_list(self, row_data, series_data_struct):
        '''把查询到的需要打平字段的列表分割、组成Series'''
        try:
            if not row_data["video_id"]:
                return row_data
            duration = self.trans_duration_2_pv(int(row_data["video_id"]))
            if duration:
                type_id = 1  # 前贴
                for item, item_data in series_data_struct.iteritems():
                    if item == "duration":
                        item_data.append(duration)
                    elif item == "type_id":
                        item_data.append(type_id)
                    else:
                        item_data.append(row_data[item])
        except Exception, exc:
            self.error("failed to fill relation: %s\n%s", exc, list(row_data.values))
        return row_data

    def __get_pv1_slot_ids(self, board_id):
        '''正一位，中贴 SlotID集合'''
        center1 = BOARD_CENTER_ONE.get(int(board_id))
        if center1 is None:
            return None
        middle_slot_ids = self.middle_slot_ids.get(center1)
        result = [center1]
        if not middle_slot_ids is None:
            result[1:] = middle_slot_ids
        return result

    def __extract_display_poss_data(self, chunk, trans_type):
        '''提取投放数据'''
        self.info("提取、打平数据库广告位ID")
        header = self.get("alg_info")[trans_type]["header"]
        series_data_struct = dict((key, []) for key in header)
        chunk.apply(self.__fill_relation_list, axis=1, series_data_struct=series_data_struct)
        self.__dtype_series(series_data_struct)
        new_chunk = pd.DataFrame(series_data_struct)
        return new_chunk

    def __extract_display_sale_data(self, chunk, trans_type):
        '''提取投放数据'''
        self.info("提取、打平请求中的广告位ID")
        self.info("transform data to display sale format")
        header = self.get("alg_info")[trans_type]["header"]
        series_data_struct = dict((key, []) for key in header)
        chunk.apply(self.__flat_slot_id, axis=1, series_data_struct=series_data_struct)
        self.__dtype_series(series_data_struct)
        new_chunk = pd.DataFrame(series_data_struct)
        return new_chunk

    def __flat_slot_id(self, row_data, series_data_struct):
        '''打平投放SlotID'''
        try:
            ad_list = urllib.unquote(row_data["ad_list"])
            if not ad_list:
                return ad_list
            list_array = ad_list.split("|")
            new_row = row_data
            for arr in list_array:
                slot_id = arr.split(",")[0]
                if (long(slot_id),) in self.p_s_slot_ids:
                    new_row["slot_id"] = slot_id
                    for key, value in series_data_struct.iteritems():
                        value.append(new_row[key])
        except Exception, exc:
            # TODO 记录出错的日志内容
            self.error("flat slot id error: %s\n%s", exc, list(row_data.values))

    def __is_board_slot_id_match(self, board_id, slot_id, server_timestamp):
        '''播放器ID和广告位ID是否匹配'''
        try:
            slot_id = int(slot_id)
            board_id = int(board_id)
            if self.player_index:
                return slot_id in self.player_slot_index[board_id]
            else:
                player_info = self.player_infos
                for vinfo in player_info.values():
                    start = vinfo.get('starttime')
                    end = vinfo.get('endtime')
                    if server_timestamp > start and server_timestamp < end:
                        if vinfo['playerinfo'].has_key(board_id):
                            if vinfo['playerinfo'][board_id].has_key(slot_id):
                                return True
        except Exception, exc:
            self.error("slot/board id match exception: %s", exc)
        return False

    def __caculate_display(self, chunk, trans_type, output_file_path, \
                           is_sum=False, sum_key=0):
        '''计算展示机会'''
        self.info("caculate display...")
        header = self.get("alg_info")[trans_type]["header"]
        write_header = self.get("alg_info")[trans_type]["write_header"]
        if chunk is None or chunk.empty:
            self.info("result is empty. write a empty result file with headers in.")
            if write_header:
                out_file = open(output_file_path, "wb")
                out_file.write(self.get("csv_sep").join(header) + self.get("csv_sep") + trans_type)
                out_file.close()
                self.get("alg_info")[trans_type].update({"write_header":False})
                return 0
        if is_sum:
            result = chunk.groupby(header, as_index=False, sort=False).sum()
        else:
            result = pd.DataFrame(chunk.groupby(header, as_index=False, sort=False).size())
            result.reset_index(inplace=True)
        result = result.rename(columns={sum_key:trans_type}).fillna(-1)
        if write_header:
            mode = 'w'
        else:
            mode = 'a'
        out_path = os.path.dirname(output_file_path)
        if not os.path.exists(out_path):
            os.makedirs(out_path)
        self.info("save result to %s", output_file_path)
        self.info("the write model is %s", mode)
        result.to_csv(output_file_path, index=False, mode=mode, \
                      header=write_header, sep=self.get("csv_sep"))
        if write_header:
            self.get("alg_info")[trans_type].update({"write_header":False})
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


    def report(self, dataframe):
        '''report result to BearyChat,Email'''

        result_size = 0
        display_sale = 0
        display_poss = 0
        if not dataframe.empty:
            result_size = len(dataframe)
            display_sale = dataframe["display_sale"].sum()
            display_poss = dataframe["display_poss"].sum()
        infos = {
             "file_name": "",
             "file_size": '%0.3f MB' % (self.get("filesize") / 1024.0 / 1024.0),
             "result_size": result_size,
             "spend_time": "%0.2f" % (self.get("end_time") - self.get("start_time")),
             "display_sale": int(display_sale),
             "display_poss": int(display_poss)
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
    def config_parmas(self, configures):
        '''config params'''

        logging.config.fileConfig("calculate/logger.conf")
        if configures.has_key("logger"):
            self.set("logger", configures.logger)
        else:
            self.set("logger", logging.getLogger('etlLogger'))

        src_file_paths = configures.get("src_files")
        if not src_file_paths or not isinstance(src_file_paths, list):
            self.error("Error src file configure: %s", src_file_paths)
            return False
        self.set("src_files", src_file_paths)

        self.set("dtype", split_header(CFG["dtype"]))
        self.set("original_error", src_file_paths[0] + CFG["original_error"])
        self.set("result_out_file", configures["result_out_file"])
        self.set("csv_sep", CFG["csv_sep"])

        return True


    def __extract_file(self, input_file_path, run_cfg):
        '''加载文件并计算'''
        self.info("extract file: %s", input_file_path)
        if os.path.exists(input_file_path):
            chunks = pd.read_csv(input_file_path, names=CFG["original_header"], \
                         chunksize=CFG["read_csv_chunksize"], \
                         sep="\n", engine='c')

            val_len = len(CFG["original_header"])
            chunk_index = 0
            # error_pool = FilterWriterPool(original_error)
            for chunk in chunks:
                self.info("handle section[%02d] %s records...", chunk_index, len(chunk))

                values = CFG["usefull_request_body_header"].values()
                req_cols = collections.OrderedDict(\
                                    (x, []) for x in values)
                # tag_list = []
                # 打平、转换、审计,用于展示机会统计
                chunk.apply(self.__split_request_body, axis=1, \
                                req_cols=req_cols, values=values, \
                                val_len=val_len)
                self.info("split completed. start to transform...")
                new_chunk = pd.DataFrame(req_cols)

                self.info("filter ad_event_type == 'e'")
                new_chunk = new_chunk[new_chunk['ad_event_type'] == 'e']

                self.transform(run_cfg, new_chunk)

                self.info("del chunk ...")
                del chunk
                del new_chunk
                chunk_index = chunk_index + 1
            for algorithm in run_cfg.iterkeys():
                if algorithm == "pv1":  # pv1需要添加数据库映射
                    dataf = pd.read_csv(run_cfg[algorithm], dtype=self.get("dtype"), \
                                     sep=self.get("csv_sep"))
                    if dataf.empty:
                        break
                    dataf["board_id"] = dataf["board_id"].fillna(-1).astype(int)
                    new_df = pd.merge(dataf, self.board_slot_id_df, on="board_id")
                    new_df["pv1"] = new_df["pv1"].fillna(0)
                    new_df = new_df.fillna(-1)
                    del new_df["board_id"]
                    del dataf
                    new_df.to_csv(run_cfg[algorithm], sep=self.get("csv_sep"), header=True, \
                        index=False)
                    del new_df
        else:
            self.error("failed to extract file :%s, file not exists.", input_file_path)
            return False
        return True

    def __split_request_body(self, row_datas, req_cols, values, val_len):
        '''拆分reqeust body'''
        # self.set("chunk_idx", self.get("chunk_idx") + 1)
        # 打平--------------------------------------Start
        try:
            row_data = row_datas[0].split(CFG["original_sep"])
            if len(row_data) != val_len:
                return row_datas
            request_body = row_data[12]
            remote_addr = row_data[0]
            http_x_forwarded_for = row_data[1]
            time_iso8601 = row_data[3]
            items = request_body.split("&")
            seri = self.__flat_datas(values, items, http_x_forwarded_for, remote_addr, time_iso8601)
        except Exception, exc:
            self.error("can not split request_body:%s\n%s", exc, row_data)
            # TODO 记录出错的日志内容
            return row_datas
        # 打平--------------------------------------End

        # 审计--------------------------------------Start
        # tag = self.__audit_orign_rows(seri)
        # 审计--------------------------------------End
        # seri["tag"] = tag
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
            seri["city_id"] = IP_UTIL.get_cityInfo_from_ip(seri["ip"], 3)
        except:
            self.error("can not transfor city id from ip: %s", seri["ip"])
            seri["city_id"] = -1
        server_timestamp = time_iso8601  # [2015-12-04T14:00:02+08:00]
        d_date = dt.datetime.strptime(server_timestamp, '[%Y-%m-%dT%H:%M:%S+08:00]').date()
        seri["server_timestamp"] = time.mktime(d_date.timetuple())
        return seri

    def __audit_orign_rows(self, row_data):
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


