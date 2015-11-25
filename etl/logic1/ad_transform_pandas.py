# encoding=utf-8
'''
使用Pandas处理CSV文件数据
'''
import os
import datetime as dt

import numpy as np
import pandas as pd

from etl.conf.settings import PdLogger as LOG
from etl.conf.settings import MONITOR_CONFIGS as CNF
from etl.util.playerutil import getplayerInfo

def split_header(header):
    '''转换配置里的数据类型、列名'''
    target_dtype = {}
    for name,the_type in header.iteritems():
        if the_type == 'int':
            target_dtype[name] = int
        elif the_type == 'string':
            target_dtype[name] = str
        elif the_type == 'list':
            target_dtype[name] = str
        elif the_type == 'long':
            target_dtype[name] = np.int64
    return target_dtype


def filter_chunk(dataframe, key, opt, val):
    '''根据条件过滤数据集'''
    LOG.info("filter column: " + key + opt + str(val))
    if '==' == opt:
        dataframe = dataframe[dataframe[key] == val]
    elif '!=' == opt:
        dataframe = dataframe[dataframe[key] != val]
    elif "in" == opt:
        dataframe = dataframe[dataframe[key].isin(val)]
    elif "<" == opt:
        dataframe = dataframe[dataframe[key] < val]
    elif ">" == opt:
        dataframe = dataframe[dataframe[key] > val]
    elif "<=" == opt:
        dataframe = dataframe[dataframe[key] <= val]
    elif "<=" == opt:
        dataframe = dataframe[dataframe[key] >= val]
    return dataframe

class AdTransformPandas(object):
    """使用Pandas处理CSV文件数据"""
    def __init__(self):
        '''初始化'''
        # 佛祖保佑，永无Bug
        buddha_bless_me()
        LOG.info("Welcome to AdTransformPandas")
        # 参数
        self.params = {}
        self.player_id_cache = None

    def calculate(self, input_path, input_filename, alg_file):
        '''计算数据
        @param input_path: 输入路径
        @param input_filename: 输入文件名
        @param alg_file: 统计指标、输出文件路径
        @return: -1 失败 0 成功
        '''
        exec_start_time = dt.datetime.now()  # 开始执行时间
        if not isinstance(alg_file, dict):
            LOG.error("非法参数alg_file：" + str(alg_file))
            return -1
        # handle path
        input_path = input_path.replace("\\", os.sep).replace("/", os.sep)
        if not input_path.endswith(os.sep):
            input_path = input_path + os.sep
        # calculate
        for algorithm in alg_file.iterkeys():
            calcu_result = self.do_calculate(algorithm, alg_file[algorithm], \
                                             input_path, input_filename)
            if calcu_result == -1:  # 出错
                LOG.error("calculate [" + algorithm + "] failed.")
        exec_end_time = dt.datetime.now()  # 结束执行时间
        exec_takes = exec_end_time - exec_start_time

        LOG.info("all task process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                str(exec_takes.seconds / 60) + " minutes)")
        return 0

    def merge_section(self):
        ''' 计量各分段数据总合 '''
        # info
        LOG.info("merge the tmp file : " + self.get('tmp_file_path'))
        # 输出结果到文件
        dataframe = pd.read_csv(self.get('tmp_file_path'), sep=self.get('output_column_sep'), \
                         names=self.get("sum_names"), header=None, dtype=self.get('dtype'))
        total_grouped = dataframe.groupby(self.get('group_item')).sum()
        del dataframe
        # merge info
        LOG.info("merge result size:" + str(len(total_grouped)))
        total_groupframe = pd.DataFrame(total_grouped)


        LOG.info("remove tmp file:" + self.get('tmp_file_path'))
        # 删除临时文件
        os.remove(self.get('tmp_file_path'))
        return total_groupframe

    def do_calculate(self, trans_type, output_file_path, input_path, input_filename):
        ''' 计算一个维度的数据 '''
        LOG.info("prepare to calculate : " + trans_type)
        conf_result = self.__configure(trans_type, output_file_path, input_path, input_filename)

        if not conf_result:
            return -1
        exec_start_time = dt.datetime.now()  # 开始执行时间
        try:
            is_load_success = self.__load_file()
            if is_load_success:
                merge_result = self.merge_section()
                self.__save_result_file(merge_result)
            else:
                LOG.error("load file failed,the file may not exists or have none data,exit.")
                return -1
        except Exception, exce:
            import traceback
            print traceback.format_exc()
            LOG.error(exce.message)
            return -1
        exec_end_time = dt.datetime.now()  # 结束执行时间

        exec_takes = exec_end_time - exec_start_time

        LOG.info("process complete in [" + str(exec_takes.seconds) + "] seconds (" + \
                str(exec_takes.seconds / 60) + " minutes)")
        return 0

    def __configure(self, trans_type, output_file_path, input_path, input_filename):
        '''配置参数'''
        self.put(("input_file_path", "output_file_path"), \
                    (os.path.join(input_path, input_filename), output_file_path))
        self.put("input_column_sep", CNF.get("input_column_sep"))
        self.put("output_column_sep", CNF.get("output_column_sep"))
        self.put("names",CNF.get("header"))
        self.put("dtype", split_header(CNF.get("header_type")))
        self.put(("chunk", "db_chunk"), (CNF.get("read_csv_chunk"), CNF.get("db_commit_chunk")))
        self.put("tmp_file_path", output_file_path + ".tmp")
        self.put("trans_type", trans_type)
        config_result = self.configure_algorithm(trans_type, CNF)
        LOG.info("configure complete, retrieve params:%s" ,
                  str(self.params))
        return config_result

    def configure_algorithm(self, trans_type, cnf):
        '''各个维度的配置'''
        algorithm = cnf.get("algorithm")
        trans_type_cnf = algorithm.get(trans_type)
        if trans_type_cnf is None:
            LOG.error("can not find algorithm type in config:" + trans_type)
            return False
        self.put("condition", trans_type_cnf.get("condition"))
        self.put("group_item", trans_type_cnf.get("group_item"))
        sum_names = []
        for item in self.get('group_item'):
            sum_names.append(item)
        for key in self.get('condition').keys():
            output_name = self.get('condition')[key]["output_name"]
            sum_names.append(output_name)
            self.get("dtype")[output_name] = np.int64
        self.put("sum_names", sum_names)
        if "display_poss" == trans_type:
            self.put("groupby_list", ['board_id', 'session_id', 'pf', \
                            'server_timestamp', 'ad_event_type', 'request_res', 'tag'])
        return True

    def __load_file(self):
        '''加载文件并计算'''
        input_file_path = self.get("input_file_path")
        LOG.info('load file:' + input_file_path)
        if os.path.exists(input_file_path):
            do_filter = True
            if "display_poss" == self.get("trans_type"):
                input_file_path = self.__transform_display_poss_file()
                do_filter = False
            if os.path.exists(input_file_path):
                # 分段处理CSV文件，每READ_CSV_CHUNK行读取一次
                data_chunks = pd.read_csv(input_file_path, sep=self.get('input_column_sep'), \
                                dtype=self.get('dtype'), index_col=False, \
                                chunksize=self.get('chunk'))
                self.__transform_section(data_chunks, do_filter)
                del data_chunks
                if "display_poss" == self.get("trans_type"):
                    os.remove(input_file_path)
                return True
            else:
                with open(self.get('output_file_path'), "wb") as fread:
                    title = self.get("output_column_sep")\
                        .join(s for s in self.get("group_item"))
                    fread.write(title + "\n")
                return False
        else:
            return False
        return True

    def __transform_display_poss_file(self):
        '''展示机会需要把打平的日志反打平'''
        LOG.info("merge display poss middle datas...")
        input_file_path = self.get("input_file_path")
        tmp_trans_file = input_file_path + ".ttmp"
        if os.path.exists(tmp_trans_file):
            os.remove(tmp_trans_file)
        LOG.info("read data from %s", input_file_path)
        data_chunks = pd.read_csv(input_file_path, sep=self.get('input_column_sep'), \
                        dtype=self.get('dtype'), index_col=False, \
                        chunksize=self.get('chunk'))
        groupby_list = self.get("groupby_list")
        trunk_size = 0
        for chunk in data_chunks:
            need_header = trunk_size == 0
            LOG.info("merge chunk %s:", trunk_size)
            trunk_size = trunk_size + 1

            # 选过滤条件，减少计算数
            condition = self.get("condition")
            for cdt_key in condition.iterkeys():
                for rel in condition[cdt_key]["filter"]:
                    key = rel[0]
                    opt = rel[1]
                    val = rel[2]
                    chunk = filter_chunk(chunk, key, opt, val)
                output_name = self.get('condition')[cdt_key]["output_name"]
                if not chunk.empty:
                    tmp_df = pd.DataFrame(chunk.groupby(groupby_list).size())\
                        .rename(columns={0:output_name})
                    LOG.info("append chunk result to %s:", tmp_trans_file)
                    tmp_df.to_csv(tmp_trans_file, header=need_header, \
                                     sep=self.get('input_column_sep'), \
                                     na_rep=CNF["na_rep"], mode="a", index=True)
        del data_chunks
        if trunk_size > 1:
            if os.path.exists(tmp_trans_file):
                data_chunks = pd.read_csv(tmp_trans_file, sep=self.get('input_column_sep'), \
                                dtype=self.get('dtype'), index_col=False)
                LOG.info("merge all trunk results")
                data_chunks = pd.DataFrame(data_chunks.groupby(groupby_list).size())\
                    .rename(columns={0:output_name})
                LOG.info("save final result to: %s", tmp_trans_file)
                data_chunks.to_csv(tmp_trans_file, header=True, \
                                         sep=self.get('input_column_sep'), \
                                         na_rep=CNF["na_rep"], index=True)
        LOG.info("merge display poss middle datas complete!")
        return tmp_trans_file
    def __transform_section(self, data_chunks, do_filter):
        '''分段转换数据'''
        LOG.info('transform...')
        ###############遍历各个分段，分段数据第一次Group Count后存入临时文件#############

        if os.path.exists(self.get('tmp_file_path')):
            LOG.info("tmp_file exists,remove : " + self.get('tmp_file_path'))
            os.remove(self.get('tmp_file_path'))
        section_index = 0

        for chunk in data_chunks:
            LOG.info("group section " + str(section_index) + " ...")
            section_index = section_index + 1
            chunk_len = len(chunk)  # 记录chunk长度
            groupframe = None
            condition = self.get("condition")
            for cdt_key in condition.iterkeys():
                groupframe = self.__calculate_algorithm(groupframe\
                                    , cdt_key, condition[cdt_key], chunk, do_filter)

            if not groupframe is None:
                # 保存到临时CSV文件
                groupframe.to_csv(self.get('tmp_file_path'), header=False, \
                                 sep=self.get('output_column_sep'), \
                                 na_rep=CNF["na_rep"], mode="a", index=True)
            elif not os.path.exists(self.get('tmp_file_path')):  # 创建一个空文件
                tmp_file = os.open(self.get('tmp_file_path'), os.O_CREAT)
                os.close(tmp_file)
            # info info
            LOG.info("grouped: " + str(chunk_len) + " records")

        LOG.info("save to tmp file : " + self.get('tmp_file_path'))
        ###############遍历各个分段，分段数据第一次Group Count后存入临时文件############
        LOG.info('transform!')

    def __calculate_algorithm(self, grouped, cdt_key, relations, tmp_chunk, do_filter):
        '''根据关系规则Group分段数据，把分段Group结果合并'''
        LOG.info("filter section columns:")
        if do_filter:
            for rel in relations["filter"]:
                key = rel[0]
                opt = rel[1]
                val = rel[2]
                tmp_chunk = filter_chunk(tmp_chunk, key, opt, val)
            LOG.info("filter section column result length: %d, result size: %d" \
                     , len(tmp_chunk), tmp_chunk.size)

        LOG.info("execute operator: " + cdt_key)
        if tmp_chunk.empty:
            return None

        if cdt_key == 'count':  # 普通计数
            grouped = pd.DataFrame(self.__merge_dataframe_group_count(grouped, tmp_chunk))
        elif cdt_key == 'query-slot-count':  # 从接口中查询slot_id再计数-->用于计算展示机会
            series_data_struct = self.__init_series_data_struct(True)
            for row in tmp_chunk.iterrows():
                self.__fill_relation_list(row[1], series_data_struct)
            self.__dtype_series(series_data_struct)
            dataf = pd.DataFrame(series_data_struct)
            grouped = self.__merge_dataframe_group_count(grouped, dataf)

        elif cdt_key == 'query-slot-compare':  # 从接口中查询slot_id比较顺序计算升位-->用于计算升位
            dataf = self.__group_slot_compare(tmp_chunk)
            grouped = self.__merge_dataframe_group_count(grouped, dataf)
        else:
            LOG.error("unsupport operation:" + cdt_key)
            return None
        return grouped

    def __merge_dataframe_group_count(self, grouped, dataframe):
        """as u c: merge dataframe group count"""
        if grouped is None:
            if not dataframe.empty and len(dataframe) > 0:
                grouped = dataframe.groupby(self.get('group_item')).size()
        else:
            grouped = pd.concat([grouped, dataframe.\
                                groupby(self.get('group_item')).size()], axis=1)
        return grouped

    def __group_slot_compare(self, chunk):
        '''按slot_id group
        @param chunk: 读取的CSV日志数据片段'''
        self.__init_series_data_struct(True, "seq", "order")
        _compare_slot_id_list = []
        for row_data in chunk.iterrows():
            row = row_data[1]
            board_id = row["board_id"]
            timestamp = float(row["server_timestamp"])
            group_id = row["group_id"]
            seq = row["seq"]  # 播放顺序
            # 获取实际在按播放顺序的广告位ID
            _compare_slot_id_list.append(self.__get_store_slotid_by_seq(board_id, timestamp, seq, group_id))
        chunk["query-slot_id"] = chunk['slot_id']
        chunk['slot_id'] = _compare_slot_id_list
        chunk = chunk[chunk['query-slot_id'] != chunk['slot_id']]
        return chunk

    def __init_series_data_struct(self, remove_prefix, *addons):
        '''初始化一个字段的Series空构造
        NOTICE:这个方法会去掉group_item、sum_names里以query-开头字段的“query-”前缀还原字段'''
        __struct = {}
        group_item = self.get("group_item")
        sum_names = self.get("sum_names")
        for item in group_item:
            if item.startswith("query-"):
                redefine_item = item.replace("query-", "", 1)
                __struct[redefine_item] = []
                if remove_prefix:
                    group_item[group_item.index(item)] = redefine_item
                    sum_names[sum_names.index(item)] = redefine_item
            else:
                __struct[item] = []
        if len(addons) > 0:
            for addon in addons:
                __struct[addon] = []
        return __struct

    def __dtype_series(self, series_data_struct):
        '''给Series构造加上数据类型(dtype)'''
        for item, arr in series_data_struct.iteritems():
            series_data_struct[item] = np.array(arr, dtype=self.get("dtype")[item])

    def __fill_relation_list(self, row_data, series_data_struct):
        '''把查询到的需要打平字段的列表分割、组成Series
            ex.目前需要打平的字段有：slot_id
         '''
        board_id = row_data["board_id"]
        timestamp = float(row_data["server_timestamp"])
        slot_ids = self.__get_slot_ids(board_id, timestamp)
        if slot_ids is None:
            LOG.error("no slot data with the condition: board_id: %s, timestamp: %s"
                       , board_id, timestamp)
            return
        for slot_id in slot_ids:
            for item, item_data in series_data_struct.iteritems():
                if item == "slot_id":
                    item_data.append(slot_id)
                else:
                    item_data.append(row_data[item])

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

    def __get_store_slotid_by_seq(self, board_id, timestamp, seq, group_id):
        '''获取一个广告位下的groupid、name和group中的排序'''
        player_infos = self.__get_player_infos()

        for group in player_infos.values():
            start = group['starttime']
            end = group['endtime']
            if start <= timestamp and end > timestamp:
                playerinfo = group["playerinfo"]
                if playerinfo.has_key(int(board_id)):
                    slot_info = playerinfo[int(board_id)]
                    for slot_id, details in slot_info.iteritems():
                        if seq is None or 'nan' == str(seq):
                            return '-1'
                        if details[0] == int(group_id) and details[2] == int(seq):
                            return str(slot_id)
        LOG.error("can not find the slot with params:board_id: %s,timestamp: %s,seq: %s", \
                  board_id, timestamp, seq)
        return '-1'

    def __get_player_infos(self):
        '''获取播放器信息，如果已经获取过，从缓存中拿'''
        if self.player_id_cache is None:
            self.player_id_cache = getplayerInfo()
        return self.player_id_cache

    def __save_result_file(self, merge_result):
        '''保存计算结果到文件'''
        LOG.info("save result to csv file:" + self.get('output_file_path'))
        merge_result.to_csv(self.get('output_file_path'), sep=\
                self.get('output_column_sep'), header=True)

    def put(self, key, value):
        '''设置self.params'''
        if isinstance(key, tuple):
            __len = len(key)
            for i in range(0, __len):
                __key = key[i]
                __value = value[i]
                self.put(__key, __value)
        else:
            self.params[key] = value

    def get(self, key):
        '''获取self.params'''
        return self.params[key]


def buddha_bless_me():
    '''佛祖保佑'''
    print r'''      #####################################################'''
    print r'''     ####################################################### '''
    print r'''    #########################################################'''
    print r'''    ##                                                     ##'''
    print r'''    ##                       _oo0oo_                       ##'''
    print r'''    ##                      o8888888o                      ##'''
    print r'''    ##                      88" . "88                      ##'''
    print r'''    ##                      (| -_- |)                      ##'''
    print r'''    ##                      0\  =  /0                      ##'''
    print r'''    ##                   ___//`---'\\___                   ##'''
    print r'''    ##                  .' \|       |/ '.                  ##'''
    print r'''    ##                 /  \|||  :  |||/  \                 ##'''
    print r'''    ##                / _||||| -:- |||||- \                ##'''
    print r'''    ##               /  _||||| -:- |||||-  \               ##'''
    print r'''    ##               | \_|  ''\---/''  |_/ |               ##'''
    print r'''    ##               \  .-\__  '-'  ___/-. /               ##'''
    print r'''    ##             ___'. .'  /--.--\  `. .'___             ##'''
    print r'''    ##          ."" '<  `.___\_<|>_/___.' >' "".           ##'''
    print r'''    ##         | | :  `- \`.;`\ _ /`;.`/ - ` : | |         ##'''
    print r'''    ##         \  \ `_.   \_ __\ /__ _/   .-` / /          ##'''
    print r'''    ##                       `=---='                       ##'''
    print r'''    ##                                                     ##'''
    print r'''    ##     Amituofo, buddha bless me, never have bug       ##'''
    print r'''    ####@#######@#######@#######@#####@#####@#######@######@#'''
    print r'''  #@##@##@####@##@####@##@####@##@##@##@##@##@####@##@###@##@##'''
    print r'''#####@####@#@#####@#@#####@#@#####@#####@#####@#@#####@#@####@###'''
