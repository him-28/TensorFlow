# encoding: utf-8
'''
etl.util.reportutil -- report ETL result to media

etl.util.reportutil is a util class to report result from
the ETL to email/bearychat with txt/img/pdf/html formats

It defines class ReportUtil

__init__ method need 3 parameters witch format as bellow example:

1. start_time: '20151012 12:20:00' or 1444623600

2. endt_time: '20151012 12:30:00' or 1444624200

3. data:
{
    border_id1:
        {
            "slot_statistics": # 展示广告位统计。广告位顺序按seq升序排列
                [
                    {
                        "slot_id": 310,
                        "slot_name": "正一",
                        "click":111,
                        "up":111,
                        "impression":111,
                        "impression_end":111,
                        "display_sale":111,
                        "display_poss":111
                    },
                    {
                        "slot_id": 311,
                        ...
                    },
                    ...
                ],
            "seq_display": [111,222,...] # 广告次序实际展示数，展示数顺序按广告位的seq升序排列
        },
    border_id2:
        {
            ...
        },
    ...
}

@author:     dico.ding

@copyright:  2015 http://www.mgtv.com. All rights reserved.

@license:    no licenses

@contact:    dingzheng@imgo.tv
'''

import datetime as dt

from etl.util import bearychat as bc

def is_num(obj):
    '''is number'''
    return isinstance(obj, int) or isinstance(obj, long)\
         or isinstance(obj, float)

class ReportUtil(object):
    '''Report ETL result'''
    def __init__(self, start_time, end_time, data):
        if is_num(start_time):
            self.start_time = dt.datetime.\
                fromtimestamp(start_time).strptime("%Y%m%d %H:%M:%S")
        else:
            self.start_time = start_time
        if is_num(end_time):
            self.end_time = dt.datetime.\
                fromtimestamp(end_time).strptime("%Y%m%d %H:%M:%S")
        else:
            self.end_time = end_time
        self.data = data

    def report_text(self, report_type="separate"):
        '''Report ETL result data with text format
        @param report_type:
            separate:每个播放器报告一个结果
            all:所有播放器合并到一个结果
        '''
        data = self.data
        result_text = []
        if report_type == "separate":  # 每个播放器分开统计
            for board_id, slot_data in data.iteritems():
                board_result = self.__statistics_board(board_id, slot_data)
                result_text .append(board_result)
        return result_text

    def __statistics_board(self, board_id, slot_data):
        '''统计一个播放器的数据，并返回统计结果'''
        format_title = "播放器【%s】展示广告位（%s~%s）统计结果：" \
            % (board_id, self.start_time, self.end_time)
        format_str = self.__statistics(slot_data)

        seq_display = slot_data["seq_display"]
        seq_len = len(seq_display)
        if seq_len > 0:
            for idx in range(0, seq_len):
                slot_str = "广告位【%s】，实际展示数：%s\n" % (idx + 1, seq_display[idx])
                format_str += slot_str
        return format_title, format_str

    def __statistics(self, slot_data, format_str=""):
        '''statistics'''
        slot_statistics = slot_data["slot_statistics"]
        if len(slot_statistics) == 0:
            format_str += "没有广告位数据"
        else:
            for data in slot_statistics:
                slot_name = data["slot_name"]
                display_poss = data["display_poss"]
                display_sale = data["display_sale"]
                click = data["click"]
                up_num = data["up"]
                impression = data["impression"]
                impression_end = data["impression_end"]
                slot_str = "广告位：%s\n#展示机会：%s,售卖展示机会：%s，\n#播放：%s，播放结束：%s，\n#点击：%s，升位：%s\n" \
                    % (slot_name, display_poss, display_sale, impression, \
                        impression_end, click, up_num)
                format_str += slot_str
        return format_str

    def report_pdf(self):
        '''Report ETL result data with pdf format'''
        pass
    def report_img(self):
        '''Report ETL result data with img format'''
        pass
    def report_html(self):
        '''Report ETL result data with html format'''
        pass

if __name__ == "__main__":
    DATA = {
                4820:
                {
                    "slot_statistics":  # 展示广告位统计。广告位顺序按seq升序排列
                    [
                        {
                            "slot_id": 310,
                            "slot_name": "正一",
                            "click":111,
                            "up":111,
                            "impression":111,
                            "impression_end":111,
                            "display_sale":111,
                            "display_poss":111
                        },
                        {
                            "slot_id": 311,
                            "slot_name": "正DG",
                            "click":111,
                            "up":111,
                            "impression":111,
                            "impression_end":111,
                            "display_sale":111,
                            "display_poss":111
                        }
                    ],
                    "seq_display": [111, 222]  # 广告次序实际展示数，展示数顺序按广告位的seq升序排列
                },
            4821:
                {
                    "slot_statistics":  # 展示广告位统计。广告位顺序按seq升序排列
                    [
                        {
                            "slot_id": 310,
                            "slot_name": "正一",
                            "click":111,
                            "up":111,
                            "impression":111,
                            "impression_end":111,
                            "display_sale":111,
                            "display_poss":111
                        },
                        {
                            "slot_id": 311,
                            "slot_name": "正一",
                            "click":111,
                            "up":111,
                            "impression":111,
                            "impression_end":111,
                            "display_sale":111,
                            "display_poss":111
                        }
                    ],
                    "seq_display": [111, 222]  # 广告次序实际展示数，展示数顺序按广告位的seq升序排列
                }
            }
    RU = ReportUtil('20151010 12:12:00', '20151010 12:13:00', DATA)
    TEXT_RESULT = RU.report_text()
    for title,text in TEXT_RESULT:
        bc.new_send_message(None, at_title=title, channel=u'Test-Dico', at_text=text)
