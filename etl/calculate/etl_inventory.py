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

from etl.util.playerutil import getplayerInfo
from etl.util.ip_convert import IP_Util

CONFIG_ALL = yaml.load(file("calculate/etl_conf.yml"))

CFG = CONFIG_ALL["inventory"]
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


class ExtractTransformLoadInventory(object):
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

        self.player_id_cache = None

        # welcome
        self.welcome()

        self.info("params configed as : %s", self.params)

    def run(self, run_cfg):
        '''Run the ETL !!!'''
        result_df = self.extract(run_cfg)  # step 1
        self.save()  # step 3
        self.end_time = time.clock()
        self.info("all task completed in [%0.2f] seconds", (self.end_time - self.start_time))
        return self.report(result_df)  # step end

    def extract(self, run_cfg, before=None, after=None):
        '''extract data file'''
        if before:
            self.__handle_before(before)
        src_files = self.get("src_files")

        result_out_file = self.get("result_out_file")
        if os.path.exists(result_out_file):
            os.remove(result_out_file)
            self.warn("output file exists, remove. %s", result_out_file)

        for file_path in src_files:
            self.__extract_file(file_path, run_cfg)
        result_df = self.__merge_chunks_result(run_cfg)

        if after:
            self.__handle_after(after)
        return result_df

    def transform(self, alg_file, chunk, before=None, after=None):
        '''transform data'''
        exec_start_time = dt.datetime.now()  # 开始执行时间

        if before:
            self.__handle_before(before)

        self.regist_alg(alg_file)

        for algorithm in alg_file.iterkeys():
            calcu_result = self.do_calculate(algorithm, alg_file[algorithm], \
                                             chunk)
            if calcu_result == -1:  # 出错
                self.error("calculate [" + algorithm + "] failed.")

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

    def do_calculate(self, trans_type, output_file_path, chunk):
        '''计算一个维度的数据 '''
        self.info("prepare to calculate : " + trans_type)
        self.info("filter tag < 100 and ad_event_type == 'e'")
        chunk = chunk[(chunk['tag'] < 100) & (chunk['ad_event_type'] == 'e')]
        try:
            if trans_type == "display_poss":
                return self.__caculate_display(chunk, trans_type, output_file_path)
            elif trans_type == "display_sale":
                chunk = self.__extract_display_sale_data(chunk, trans_type)
                return self.__caculate_display(chunk, trans_type, output_file_path)
        except Exception, exc:
            self.error("calculate error. %s ", exc)
            return -1
        return -1

    def __extract_display_sale_data(self, chunk, trans_type):
        '''提取投放数据'''
        self.info("transform data to display sale format")
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
            server_timestamp = float(row_data["server_timestamp"])
            if not ad_list:
                return ad_list
            list_array = ad_list.split("|")
            new_row = row_data
            for arr in list_array:
                slot_id = arr.split(",")[0]
                new_row["slot_id"] = slot_id
                if self.__is_board_slot_id_match(board_id, slot_id, server_timestamp):
                    for key, value in series_data_struct.iteritems():
                        value.append(new_row[key])
                else:
                    # TODO record the row
                    pass
        except Exception, exc:
            # TODO 记录出错的日志内容
            self.error("flat slot id error: %s\n%s", exc, list(row_data.values))

    def __is_board_slot_id_match(self, board_id, slot_id, server_timestamp):
        try:
            slot_id = int(slot_id)
            board_id = int(board_id)
            player_info = self.__get_player_infos()
            for v in player_info.values():
                start = v.get('starttime')
                end = v.get('endtime')
                if server_timestamp > start and server_timestamp < end:
                    if v['playerinfo'].has_key(board_id):
                        if v['playerinfo'][board_id].has_key(slot_id):
                            return True
        except Exception, exc:
            self.error("slot/board id match exception: %s", exc)
        return False

    def __caculate_display(self, chunk, trans_type, output_file_path):
        '''计算展示机会'''
        self.info("caculate display...")
        header = self.get("alg_info")[trans_type]["header"]
        series_data_struct = dict((key, []) for key in header)
        chunk.apply(self.__fill_relation_list, axis=1, \
                     series_data_struct=series_data_struct)
        self.__dtype_series(series_data_struct)
        dataframe = pd.DataFrame(series_data_struct)
        result = pd.DataFrame(dataframe.groupby(header, as_index=False, sort=False).size())
        write_header = self.get("alg_info")[trans_type]["write_header"]
        if result.empty:
            if write_header:
                out_file = open(output_file_path, "wb")
                out_file.write(self.get("csv_sep").join(header))
                out_file.close()
        else:
            if write_header:
                mode = 'w'
            else:
                mode = 'a'
            out_path = os.path.dirname(output_file_path)
            if not os.path.exists(out_path):
                os.makedirs(out_path)
            self.info("save result to %s", output_file_path)
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
        '''把查询到的需要打平字段的列表分割、组成Series
            ex.目前需要打平的字段有：slot_id
         '''
        try:
            board_id = row_data["board_id"]
            timestamp = float(row_data["server_timestamp"])
            if not timestamp:
                timestamp = -1
            slot_ids = self.__get_slot_ids(board_id, timestamp)
            if slot_ids is None:
                self.debug("no slot data with the condition: board_id: %s, timestamp: %s"
                           , board_id, timestamp)
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


    def save(self, before=None, after=None):
        '''save result into DB'''
        if before:
            self.__handle_before(before)
        if after:
            self.__handle_after(after)

    def report(self, dataframe, before=None, after=None):
        '''report result to BearyChat,Email'''
        if before:
            self.info("before report: %s", before)
            self.__handle_before(before)


        result_size = 0
        display_sale = 0
        display_poss = 0
        if not dataframe.empty:
            result_size = len(dataframe)
            display_sale = dataframe["display_sale"].sum()
            display_poss = dataframe["display_poss"].sum()
        infos = {
             "file_name": "",
             "file_size": "",
             "result_size": result_size,
             "spend_time": "%0.2f" % (self.end_time - self.start_time),
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


        if after:
            self.info("after report: %s", after)
            self.__handle_after(after)

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
                         sep=CFG["original_sep"], engine='python')

            # error_pool = FilterWriterPool(original_error)
            for chunk in chunks:
                self.info("handle %s records...", len(chunk))
                new_chunk = pd.DataFrame()

                # self.set("chunk_idx", -1)
                keys = CFG["usefull_request_body_header"].keys()
                values = CFG["usefull_request_body_header"].values()
                req_cols = collections.OrderedDict(\
                                    (x, []) for x in values)

                tag_list = []
                # 打平、转换、审计,用于展示机会统计
                chunk.apply(self.__split_request_body, axis=1, \
                                req_cols=req_cols, keys=keys, values=values, tag_list=tag_list)

                for name, datas in req_cols.iteritems():
                    new_chunk[name] = datas

                self.transform(run_cfg, new_chunk)
        else:
            self.error("failed to extract file :%s, file not exists.", input_file_path)
            return False
        return True

    def __merge_chunks_result(self, run_cfg):
        '''merge'''
        self.info("merge chunk files...")
        dataframe_list = []
        for key, result_path in run_cfg.iteritems():
            header = self.get("alg_info")[key]["header"]
            dataframe = pd.read_csv(result_path, engine='c', dtype=self.get("dtype"), \
                            sep=self.get('csv_sep'))\
                                    .groupby(header, as_index=False, sort=False).sum()
            dataframe = pd.DataFrame(dataframe).rename(columns={'0':key})
            dataframe_list.append(dataframe)
            self.info("save %s to %s", key, result_path)
            dataframe.to_csv(result_path, index=False, \
                             header=True, sep=self.get("csv_sep"))
        result_df = pd.concat(dataframe_list, ignore_index=True)
        result_out_file = self.get("result_out_file")
        self.info("merge result to %s", result_out_file)
        result_df.to_csv(result_out_file, sep=self.get("csv_sep"), index=False)
        return result_df

    def __split_request_body(self, row_data, req_cols, keys, values, tag_list):
        '''拆分reqeust body'''
        # self.set("chunk_idx", self.get("chunk_idx") + 1)
        # 打平--------------------------------------Start
        try:
            request_body = row_data["request_body"]
            remote_addr = row_data["remote_addr"]
            http_x_forwarded_for = row_data["http_x_forwarded_for"]
            time_iso8601 = row_data["time_iso8601"]
            items = request_body.split("&")
            seri = self.__flat_datas(values, items, http_x_forwarded_for, remote_addr, time_iso8601)
        except Exception, exc:
            self.error("can not split request_body:%s\n%s", exc, request_body)
            # TODO 记录出错的日志内容
            return request_body
        # 打平--------------------------------------End

        # 审计--------------------------------------Start
        tag = self.__audit_orign_rows(seri, tag_list)
        # 审计--------------------------------------End
        seri["tag"] = tag
        for key, value in req_cols.iteritems():
            value.append(seri[key])
        return request_body

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
        if http_x_forwarded_for == "-":
            seri["ip"] = remote_addr
        else:  # 代理第一跳
            seri["ip"] = http_x_forwarded_for.split(",")[0]

        seri["city_id"] = IP_UTIL.get_cityInfo_from_ip(seri["ip"], 3)
        server_timestamp = time_iso8601  # [2015-12-04T14:00:02+08:00]
        d_date = dt.datetime.strptime(server_timestamp, '[%Y-%m-%dT%H:%M:%S+08:00]').date()
        seri["server_timestamp"] = time.mktime(d_date.timetuple())
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
        player_infos = self.__get_player_infos()

        for group in player_infos.values():
            start = group['starttime']
            end = group['endtime']
            if start <= timestamp and end > timestamp:
                playerinfo = group["playerinfo"]
                if playerinfo.has_key(int(board_id)):
                    return playerinfo[int(board_id)].keys()
        return None

    def __get_player_infos(self):
        '''获取播放器信息，如果已经获取过，从缓存中拿'''
        if self.player_id_cache is None:
            self.player_id_cache = getplayerInfo()
        return self.player_id_cache

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

