# encoding: utf-8
'''
etl.util.reportutil -- report ETL result to media

etl.util.reportutil is a util class to report result from
the ETL to email/bearychat with txt/img/pdf/html formats

It defines class Reportor

__init__ method need 3 parameters witch format as bellow example:

#1. start_time: '20151012 12:20:00' or 1444623600

1. endt_time: '20151012 12:30:00' or 1444624200

2. data:
{
    pf1:
        {
        border_id1:
            {
                "slot_statistics": # 展示广告位统计。广告位顺序按seq升序排列
                    [
                        {
                            "slot_id": 310,
                            "slot_name": "正一",
                            "click":{
                                "logic0":111,
                                "logic1":222,
                            },
                            "up":{
                                "logic0":111,
                                "logic1":222,
                            },
                            "impression":{
                                "logic0":111,
                                "logic1":222,
                            },
                            "impression_end":{
                                "logic0":111,
                                "logic1":222,
                            },
                            "display_sale":{
                                "logic0":111,
                                "logic1":222,
                            },
                            "display_poss":{
                                "logic0":111,
                                "logic1":222,
                            }
                        },
                        {
                            "slot_id": 311,
                            ...
                        },
                        ...
                    ],
                "seq_display": {
                            1:
                                {
                                    "logic0":111,
                                    "logic1":222,
                                },
                            2：
                                {
                                    "logic0":111,
                                    "logic1":222,
                                },...} # 广告次序实际展示数
            },
        border_id2:
            {
                ...
            },
        ...
        },
    pf2...
}

@author:     dico.ding

@copyright:  2015 http://www.mgtv.com. All rights reserved.

@license:    no licenses

@contact:    dingzheng@imgo.tv
'''

import datetime as dt

from etl.conf.settings import LOGGER as LOG
from etl.conf.settings import MONITOR_CONFIGS
from etl.util import bearychat as bc
from etl.util.playerutil import getplayerInfo

REPORT_CHANNEL = MONITOR_CONFIGS["bearychat_channel"]

def is_num(obj):
    '''is number'''
    return isinstance(obj, int) or isinstance(obj, long)\
         or isinstance(obj, float)

PF = {
        "000000":"PC Web", "000100":"PC Client for Mac", \
        "000101":"PC Client for Windows", "010000":" iPad App", \
        "010001":"Android Pad App", "010100":"iPhone App", \
        "010101":"Android Phone App", "010200":"Mobile Web for Phone", \
        "010201":"Mobile Web for Pad", "020000":"OTT TV"
}

def get_pf_name(pf_code):
    '''get pf name by code'''
    if not pf_code:
        return "空结果"
    pf_code = str(pf_code)
    if PF.has_key(pf_code):
        return PF[pf_code]
    return "未知：" + pf_code

def get_metric_data(metric, logic, data):
    '''get data in each metric'''
    if data.has_key(metric):
        if data[metric].has_key(logic):
            return int(data[metric][logic])
        else:
            LOG.debug("can not find logic:%s in metric:%s,the data is:%s", \
                      logic, metric, data)
    else:
        LOG.debug("can not find metric:%s,the data is:%s", metric, data)
    return 0

class Reportor(object):
    '''Report ETL result'''
    def __init__(self, params, data):
        self.params = params
        start_time = params["start_time"]
        LOG.info("init reportor.")
        if is_num(start_time):
            self.start_time = dt.datetime.\
                fromtimestamp(start_time).strptime("%Y%m%d %H:%M:%S")
        else:
            self.start_time = start_time
        self.data = data
        self.total = {}
        for _pf in data.keys():
            self.__put(_pf, ("display_poss0", "display_poss1", \
                        "click0", "click1", \
                        "impression0", "impression1", \
                        "impression_rate0", "impression_rate1", \
                        "click_rate0", "click_rate1", \
                        "display_sale0", "display_sale1", \
                        "display0", "display1", \
                        "impression_end0", "impression_end1", \
                        "up0", "up1"), \
                       (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

    def report_text(self):
        '''Report ETL result data in text format
        '''
        data = self.data
        LOG.info("report text...")
        LOG.info("got data:")

        result_text = []
        if data:
            for _pf, pf_data in data.iteritems():
                _pf_result_text = []
                for board_id, slot_data in pf_data.iteritems():
                    _pf_result_text.extend(self.__statistics(_pf, board_id, slot_data))
                    _pf_result_text.extend(self.__seqs(board_id, slot_data))
                _pf_result_text.insert(0, self.__report_total_text(_pf))
                result_text.append((_pf, _pf_result_text))
        else:
            result_text = [(None, [("没有数据", "审计结果是空")])]

        # 发送到Bearychat
        for _pf, t_r in result_text:
            msg = ""
            for title, text in t_r:
                msg += title + "\n"
                msg += text + "\n"
            if self.params['type'] == 'day':
                time_title = "【%s】【%s】天数据统计完成" % (_pf,self.start_time)
            else:
                time_title = "【%s】【%s】小时数据统计完成" % (_pf,self.start_time)
            LOG.info("report text,title: %s .", time_title)
            bc.new_send_message(text=get_pf_name(_pf), at_title=time_title, \
                                channel=REPORT_CHANNEL , at_text=msg)
        return result_text

    def __report_total_text(self, _pf):
        '''汇总报告'''
#         display_poss0 = self.__get(_pf, "display_poss0")
        display_poss1 = self.__get(_pf, "display_poss1")
#         impression0 = self.__get(_pf, "impression0")
        impression1 = self.__get(_pf, "impression1")
#         click0 = self.__get(_pf, "click0")
        click1 = self.__get(_pf, "click1")
#         display_sale0 = self.__get(_pf, "display_sale0")
        display_sale1 = self.__get(_pf, "display_sale1")
#         up0 = self.__get(_pf, "up0")
        up1 = self.__get(_pf, "up1")
#         impression_end0 = self.__get(_pf, "impression_end0")
        impression_end1 = self.__get(_pf, "impression_end1")
#         impression_rate0 = 0.0
        impression_rate1 = 0.0
#         click_rate0 = 0.0
        click_rate1 = 0.0
#         if not display_poss0 == 0:
#             impression_rate0 = 100.0 * impression0 / display_poss0
        if not display_sale1 == 0:
            impression_rate1 = 100.0 * impression1 / display_sale1
#         if not impression0 == 0:
#             click_rate0 = 100.0 * click0 / impression0
        if not impression1 == 0:
            click_rate1 = 100.0 * click1 / impression1
#         slot_title = "【展示机会】：  %s \n【投放数】：  %s \n【开始播放数】：  %s \n\
# 【播放结束数】： %s \n【点击数】： %s \n【升位数】： %s \n【曝光率】： %s \n【点击率】： %s \n"
#         slot_title = "【投放数】：  %s \n【开始播放数】：  %s \n\
# 【播放结束数】： %s \n【点击数】： %s \n【升位数】： %s \n【曝光率】： %s \n【点击率】： %s \n"
        slot_title = " %s , %s , %s , %s , %s , %s , %s \n"
#         l0_1 = "logic0总计： %s ，logic1总计：  %s"
#         f0_1 = "logic0总计： %.2f%% ，logic1总计：  %.2f%%"
        l0_1 = " %s"
        f0_1 = " %.2f%%"
#         slot_value = (
#                       l0_1 % (display_poss0, display_poss1), \
#                       l0_1 % (display_sale0, display_sale1), \
#                       l0_1 % (impression0, impression1), \
#                       l0_1 % (impression_end0, impression_end1), \
#                       l0_1 % (click0, click1), \
#                       l0_1 % (up0, up1), \
#                       f0_1 % (impression_rate0, impression_rate1), \
#                       f0_1 % (click_rate0, click_rate1))
        slot_value = (
                              l0_1 % ( display_sale1), \
                              l0_1 % ( impression1), \
                              l0_1 % ( impression_end1), \
                              l0_1 % ( click1), \
                              l0_1 % ( up1), \
                              f0_1 % ( impression_rate1), \
                              f0_1 % ( click_rate1))
        slot_str = slot_title % slot_value
        fnssp = ""
        if "hour" == self.params["type"]:
            fnssp += "● 文件名{%s}, 大小%s \r\n" % (self.params["filename"], self.params["filesize"])
#             fnssp += "● logic0 耗时 %s秒\r\n● logic1 耗时 %s秒 \r\n" % \
#                 (str(self.params["logic0_sptime"]), str(self.params["logic1_sptime"]))
            fnssp += "● 统计 耗时 %s秒 \r\n" % \
                ( str(self.params["logic1_sptime"]))
            fnssp += "● 数据顺序：投放数，开始播放数，结束播放数，点击，升位，投放成功率，点击率\r\n"
        elif "day" == self.params["type"]:
            sptime = self.params["sptime"]
            fnssp = "● 耗时：%s秒 \r\n" % sptime
            fnssp += "● 数据顺序：投放数，开始播放数，结束播放数，点击，升位，投放成功率，点击率\r\n"
        return fnssp + "【汇总报告】", slot_str

    def __statistics(self, _pf, board_id, slot_data):
        '''statistics'''

        if not slot_data.has_key("slot_statistics"):
            return [("播放器ID【%s】" % board_id, "没有广告位数据")]

        slot_statistics = slot_data["slot_statistics"]
        if len(slot_statistics) == 0:
            return [("播放器ID【%s】" % board_id, "没有广告位数据")]
        else:
            result = []
            for data in slot_statistics:
#                 display_poss0 = get_metric_data("display_poss", "logic0", data)
                display_poss1 = get_metric_data("display_poss", "logic1", data)
#                 self.__sum_put(_pf, ("display_poss0", "display_poss1"), \
#                                 (display_poss0, display_poss1))
                self.__sum_put(_pf, "display_poss1", \
                                display_poss1)
#                 click0 = get_metric_data("click", "logic0", data)
                click1 = get_metric_data("click", "logic1", data)
#                 self.__sum_put(_pf, ("click0", "click1"), (click0, click1))
                self.__sum_put(_pf, "click1", click1)
#                 impression0 = get_metric_data("impression", "logic0", data)
                impression1 = get_metric_data("impression", "logic1", data)
#                 self.__sum_put(_pf, ("impression0", "impression1"), (impression0, impression1))
                self.__sum_put(_pf, "impression1", impression1)
#                 slot_title = "【展示机会】 %s \n【投放数】 %s \n【开始播放数】 %s \n\
# 【播放结束数】 %s \n【点击数】 %s \n【升位数】 %s \n【曝光率】 %s \n【点击率】 %s\n"
                slot_title = " %s , %s , %s , %s , %s , %s , %s"
                impression_rate0 = 0
                impression_rate1 = 0
                click_rate0 = 0.0
                click_rate1 = 0.0
#                 if not display_poss0 == 0:
#                     impression_rate0 = 100.0 * impression0 / display_poss0
                
#                 if not impression0 == 0:
#                     click_rate0 = 100.0 * click0 / impression0
                if not impression1 == 0:
                    click_rate1 = 100.0 * click1 / impression1
#                 display_sale0 = get_metric_data("display_sale", "logic0", data)
                display_sale1 = get_metric_data("display_sale", "logic1", data)
#                 self.__sum_put(_pf, ("display_sale0", "display_sale1"), \
#                                 (display_sale0, display_sale1))
                if not display_sale1 == 0:
                    impression_rate1 = 100.0 * impression1 / display_sale1
                    
                self.__sum_put(_pf, "display_sale1", \
                                display_sale1)
#                 impression_end0 = get_metric_data("impression_end", "logic0", data)
                impression_end1 = get_metric_data("impression_end", "logic1", data)
#                 self.__sum_put(_pf, ("impression_end0", "impression_end1"), \
#                                (impression_end0, impression_end1))
                self.__sum_put(_pf, "impression_end1", \
                               impression_end1)
#                 up0 = get_metric_data("up", "logic0", data)
                up1 = get_metric_data("up", "logic1", data)
#                 self.__sum_put(_pf, ("up0", "up1"), (up0, up1))
                self.__sum_put(_pf, "up1", up1)
#                 l0_1 = "logic0总计： %s， logic1总计： %s"
#                 f0_1 = "logic0总计： %.2f%%， logic1总计： %.2f%%"
                l0_1 = "%s"
                f0_1 = " %.2f%%"
#                 slot_value = (
#                               l0_1 % (display_poss0, display_poss1), \
#                               l0_1 % (display_sale0, display_sale1), \
#                               l0_1 % (impression0, impression1), \
#                               l0_1 % (impression_end0, impression_end1), \
#                               l0_1 % (click0, click1), \
#                               l0_1 % (up0, up1), \
#                               f0_1 % (impression_rate0, impression_rate1), \
#                               f0_1 % (click_rate0, click_rate1))
                slot_value = (
                              l0_1 % ( display_sale1), \
                              l0_1 % ( impression1), \
                              l0_1 % ( impression_end1), \
                              l0_1 % ( click1), \
                              l0_1 % ( up1), \
                              f0_1 % ( impression_rate1), \
                              f0_1 % ( click_rate1))
                slot_str = slot_title % slot_value
#                 format_title = "播放器ID【%s】，展示广告位：【%s】 " % (board_id, data["slot_name"])
                format_title = "%s,%s " % (board_id, data["slot_name"])
                result.append((format_title, slot_str))
        return result

    def __seqs(self, board_id, slot_data):
        '''statistics'''

        if (not slot_data.has_key("seq_display")) or len(slot_data["seq_display"]) == 0:
            return [("播放器ID【%s】" % board_id, "没有顺序位展示数据\n")]

        seq_display = slot_data["seq_display"]
#         format_title = "播放器ID【%s】" % board_id
#         format_title = "播放器ID【%s】,广告顺序位展示数" % board_id
        format_title = "%s,广告顺序位展示数" % board_id
        seq_list = []
        # 把dict转换成list
        for seq, data in seq_display.iteritems():
            seq_list.append((seq,data))
        # 对list排序
        seq_len = len(seq_list)
        for i in range(0,seq_len):
            for j in range(0,seq_len):
                seq1 = seq_list[i][0]
                if not seq1:
                    seq1 = 0
                else:
                    seq1 = int(seq1)
                seq2 = seq_list[j][0]
                if not seq2:
                    seq2 = 0
                else:
                    seq2 = int(seq2)
                if seq1 < seq2:
                    tmp = seq_list[i]
                    seq_list[i] = seq_list[j]
                    seq_list[j] = tmp
        # 拼接成字符串
        seq_str = ""
        for i in range(0,seq_len):
            seq = seq_list[i][0]
            data = seq_list[i][1]
#             logic0_data = 0
#             if data.has_key("logic0"):
#                 logic0_data = data["logic0"]
            logic1_data = 0
            if data.has_key("logic1"):
                logic1_data = data["logic1"]
#             seq_str += "【广告位顺序 ~ %s】实际展示数：logic0总计: %s，logic1总计: %s \n"\
#                 % (seq, logic0_data, logic1_data)
            seq_str += " %s ,"\
                % (logic1_data)
            
        return [(format_title, seq_str+"\n")]

    def __get(self, _pf , total_key):
        '''get'''
        return self.total[_pf][total_key]

    def __put(self, _pf, total_key, total_value):
        '''set'''

        if not self.total.has_key(_pf):
            self.total[_pf] = {}

        if isinstance(total_key, tuple):
            __len = len(total_key)
            for i in range(0, __len):
                __key = total_key[i]
                __value = total_value[i]
                self.__put(_pf, __key, __value)
        else:
            self.total[_pf][total_key] = total_value

    def __sum_put(self, _pf, total_key, add_total_value):
        '''原有值Sum后put'''
        if isinstance(total_key, tuple):
            __len = len(total_key)
            for i in range(0, __len):
                __key = total_key[i]
                __value = add_total_value[i]
                self.__sum_put(_pf, __key, __value)
        else:
            self.total[_pf][total_key] = self.total[_pf][total_key] + add_total_value

    def report_pdf(self):
        '''Report ETL result data with pdf format'''
        pass
    def report_img(self):
        '''Report ETL result data with img format'''
        pass
    def report_html(self):
        '''Report ETL result data with html format'''
        pass



import pandas as pd

from etl.logic1.ad_transform_pandas import split_header
from etl.conf.settings import MONITOR_CONFIGS as CNF


class DataReader(object):
    '''读取计算好的结果'''
    def __init__(self):
        self.data_struct = {}
        self.dtype = split_header(CNF.get("header_type"))
        self.sep = CNF.get("output_column_sep")
        self.__player_id_cache = None
        self.__slot_id_cache = {}

    def hour_data(self, paths0, paths1):
        '''按小时计的结果'''
        LOG.info("create hour data structs ...")
#         for key, path in paths0.iteritems():
#             if key == "display":
#                 dataf = self.__get_seq_data_frame(path)
#                 if not dataf is None:
#                     self.__handle_seq_data(dataf, "logic0")
#             else:
#                 dataf = self.get_data_frame(path)
#                 if not dataf is None:
#                     self.__handle_metric_data(key, dataf, "logic0")

        for key, path in paths1.iteritems():
            if key == "display":
                dataf = self.__get_seq_data_frame(path)
                if not dataf is None:
                    self.__handle_seq_data(dataf, "logic1")
            else:
                dataf = self.get_data_frame(path)
                if not dataf is None:
                    self.__handle_metric_data(key, dataf, "logic1")
        return self.data_struct

    def get_data_frame(self, data_file_path):
        '''謧文件返回统一的统计合集'''
        LOG.info("retieve data frame : %s", data_file_path)
        dataf = pd.read_csv(data_file_path, sep=self.sep, \
                            dtype=self.dtype, index_col=False)
        if dataf.empty:
            LOG.info("got empty dataframe!")
            return None
        return dataf.groupby(['board_id', 'pf', 'slot_id']).sum()

    def __get_seq_data_frame(self, data_file_path):
        '''謧文件返回统一的顺序实际展示数统计合集'''
        LOG.info("retieve seq data frame : %s", data_file_path)
        dataf = pd.read_csv(data_file_path, sep=self.sep, \
                            dtype=self.dtype, index_col=False)
        if dataf.empty:
            LOG.info("got empty seq dataframe!")
            return None
        return dataf.groupby(['board_id', 'pf', 'seq']).sum()

    def __handle_seq_data(self, dataf, logic):
        '''返回顺序实际展示数的统计数据格式'''
        LOG.info("handle seq metric data")
        for row in dataf.iterrows():
            board_id = int(row[0][0])
            _pf = str(row[0][1])
            seq = str(row[0][2])
            total = row[1]["total"]
            seq_displays = self.__get_seq_display(_pf, board_id)
            if seq_displays.has_key(seq):
                seq_displays[seq][logic] = total
            else:
                seq_displays[seq] = {logic:total}

    def __handle_metric_data(self, metric, dataf, logic):
        '''返回统一的统计数据格式'''
        LOG.info("handle metric data")
        for row in dataf.iterrows():
            board_id = int(row[0][0])
            _pf = str(row[0][1])
            slot_id = int(row[0][2])
            total = row[1]["total"]  # TODO FIXME int compareS
            has_update = False
            slot_statistics = self.__get_slot_statistics(_pf, board_id)
            for s_slot_info in slot_statistics:
                if int(s_slot_info["slot_id"]) == slot_id:  # TODO FIXME int compareS
                    if not s_slot_info.has_key(metric):
                        s_slot_info[metric] = {logic:total}
                    else:
                        s_slot_info[metric][logic] = total
                    has_update = True
                    break
            if not has_update:
                slot_statistics.append({
                            "slot_id": slot_id,
                            "slot_name": self.__get_slot_name(slot_id),
                            metric:{
                                logic:total
                            }
                        })

    def __get_slot_statistics(self, _pf, board_id):
        '''获取已存在的或新建统计结果返回'''
        if not self.data_struct.has_key(_pf):
            self.data_struct[_pf] = {}
        if not self.data_struct[_pf].has_key(board_id):
            self.data_struct[_pf][board_id] = {}
        if not self.data_struct[_pf][board_id].has_key("slot_statistics"):
            self.data_struct[_pf][board_id]["slot_statistics"] = []
        return self.data_struct[_pf][board_id]["slot_statistics"]

    def __get_seq_display(self, _pf, board_id):
        '''获取已存在的或新建实际展示数统计结果返回'''
        if not self.data_struct.has_key(_pf):
            self.data_struct[_pf] = {}
        if not self.data_struct[_pf].has_key(board_id):
            self.data_struct[_pf][board_id] = {}
        if not self.data_struct[_pf][board_id].has_key("seq_display"):
            self.data_struct[_pf][board_id]["seq_display"] = {}
        return self.data_struct[_pf][board_id]["seq_display"]


    def __get_player_infos(self):
        '''获取播放器信息，如果已经获取过，从缓存中拿'''
        if self.__player_id_cache is None:
            LOG.info("try to retrieve player info...")
            self.__player_id_cache = getplayerInfo()  # TODO FIXME 更改了Status的Player可能获取不到（存在时间差）
            LOG.info("got : %s", str(self.__player_id_cache))
        return self.__player_id_cache

    def __get_slot_name(self, slot_id):
        '''根据slot_id获取slot name'''
        if self.__slot_id_cache.has_key(slot_id):
            return self.__slot_id_cache[slot_id]
        else:
            pinfo = self.__get_player_infos()
            if pinfo is None:
                LOG.error("no player info,plz check the db connection")
                return str(slot_id)
            for item in pinfo.values():
                playerinfo = item["playerinfo"]
                for group_id, slot_infos in playerinfo.iteritems():
                    for _id, slot_info in slot_infos.iteritems():
                        if str(_id) == str(slot_id):
                            res = slot_info[1]
                            self.__slot_id_cache[slot_id] = res
                            return res
        return str(slot_id)

