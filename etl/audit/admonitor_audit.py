#encoding=utf-8
# author: martin

import sys
import time
import pandas as pd
import numpy as np

from datetime import datetime

from etl.util.csvvalidator import CSVValidator, enumeration, number_range_inclusive,\
        unsignedint_inclusive, match_pattern, str_len, str_len_range, \
        write_problems, emit_problems, datetime_string, RecordError
from etl.util.bearychat import new_send_message

PF = ('000000', '000100', '000101', '010000', '010001', '010101', '010200', '010201', '020000')
NET = ('WIFI', 'wifi', '3g', '3G', '4g', '4G', '2g', '2G', u'蜂窝')

def admonitor_csv_validator():
    """Create AD Monitor CSV validator for data"""
    field_names = (
            u'ip',
            u'province',
            u'city',
            u'ad_event_type',
            u'url',
            u'video_id',
            u'playlist_id',
            u'board_id',
            u'request_res',
            u'ad_list',
            u'time_delay',
            u'request_str',
            u'slot_id',
            u'compaign_id',
            u'creator_id',
            u'video_play_time',
            u'order',
            u'group_id',
            u'play_event',
            u'pf',
            u'device_id',
            u'uid',
            u'net',
            u'os',
            u'manufacturer',
            u'model',
            u'app',
            u'timestamp',
            u'session_id',
            u'tag'
            )

    validator = CSVValidator(field_names)
    
    #basic header and record length check
    validator.add_header_check('EX1', 'bad header')
    validator.add_record_length_check('EX2', 'bad header')

    #value check

    validator.add_value_check('ip', unsignedint_inclusive,
                              'EX3', 'invalid ip')
    #validator.add_value_check('province', unsignedint_inclusive,
    #                          'EX4', 'invalid city')
    #validator.add_value_check('city', unsignedint_inclusive,
    #                          'EX5', 'invalid city')
    validator.add_value_check('ad_event_type', enumeration('e', 'p'),
                              'EX6', 'invalid ad_event_type')
    validator.add_value_check('url', match_pattern(r"^http"),
                              'EX7', 'invalid url')
    validator.add_value_check('video_id', unsignedint_inclusive,
                              'EX8', 'invalid video_id')
    validator.add_value_check('playlist_id', unsignedint_inclusive,
                              'EX9', 'invalid playlist_id')
    validator.add_value_check('board_id', unsignedint_inclusive,
                              'EX10', 'invalid board_id')
    validator.add_value_check('time_delay', unsignedint_inclusive,
                              'EX11', 'invalid time_delay')
    validator.add_value_check('request_str', match_pattern(r"^http"),
                              'EX12', 'invalid request_str')
    validator.add_value_check('slot_id', unsignedint_inclusive,
                              'EX13', 'invalid slot_id')
    validator.add_value_check('compaign_id', unsignedint_inclusive,
                              'EX14', 'invalid compaign_id')
    validator.add_value_check('creator_id', unsignedint_inclusive,
                              'EX15', 'invalid creator_id')
    validator.add_value_check('video_play_time', unsignedint_inclusive,
                              'EX16', 'invalid video_play_time')
    validator.add_value_check('order', unsignedint_inclusive,
                              'EX17', 'invalid order')
    validator.add_value_check('group_id', unsignedint_inclusive,
                              'EX18', 'invalid group_id')
    validator.add_value_check('play_event', enumeration('s', 'e', 'c', 'sk', 'p', 'up', 'm', 'um'),
                              'EX19', 'invalid play_event')
    validator.add_value_check('pf', enumeration(PF),
                              'EX20', 'invalid pf')
    #validator.add_value_check('device_id', str_len_range(40, 48),
    #                          'EX21', 'invalid device_id')
    validator.add_value_check('net', enumeration(NET),
                              'EX22', 'invalid net')
    validator.add_value_check('session_id', str_len(36),
                              'EX23', 'invalid uuid')
    validator.add_value_check('tag', number_range_inclusive(0,1000, int),
                              'EX24', 'invalid tag')

    return validator



fmt = ""
class ADMonitorAuditRobot(object):
    def __init__(self, total, problems, spent):
        """
        Record Total: total
        Problems: problems
        spent time: spent
        """
        self.total = total
        self.problems = problems
        self.spent = spent
        self.title = u"小金汇报-android 手机"
        self.channel = u'广告-数据'
        self.normal = '#F8F8FF'
        self.error = '#FF0000'
        self.success = '#7FFFD4'

    def report(self):
        m_title = u'%s 数据审计完成' % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        content = u"总共%d纪录, 发现%d行错误,共耗时%d秒\r\n" % (self.total, self.problems.get('total', 0), self.spent)

        column_total = self.problems['column_total']
        sample = self.problems['sample']

        for k, v in column_total.iteritems():
            content += u"字段: %s, 错误次数: %d\r\n" % (k, v)

        content += u"错误抽样样本%d条\r\n" % len(sample)

        for i in sample:
            content += u"第%d行, 字段:%s, 值:%s, 错误信息:%s" % (i['row'], i['field'], i['value'], i['message'])

        if self.problems.get('total', 0) > 0:
            new_send_message(self.title , True, self.channel, m_title, content, self.error)
        else:
            new_send_message(self.title , True, self.channel, m_title, content, self.success)

def main(path):

    start = time.clock()
    df = pd.read_csv(path, sep="\t", chunksize=100000, index_col=False, encoding="utf-8", dtype={'pf': np.str})

    err_problems = []

    raw_data = pd.DataFrame()

    count = 0
    for chunk in df:
        raw_data = pd.concat([chunk, raw_data])
        count += len(chunk)
        validator = admonitor_csv_validator()
        problems = validator.validate(chunk,
                                    summarize= False,
                                    report_unexpected_exceptions=False,
                                    context={'file': True})
        
        #DEBUG Model
        err_problems += problems
        write_problems(problems,
                    sys.stdout,
                    summarize= False,
                    limit=0)


    total = set()
    sample = list()
    column_total = dict()

    for p in err_problems:
        if p.has_key('row') and p.has_key('field'):

            total.add(p['row'])

            if len(sample) < 10:
                sample.append({
                    'row': p['row'],
                    'field': p['field'],
                    'value': p['value'],
                    'message': p['message']
                    })
            field_count = column_total.get(p['field'], 0)
            column_total.update({
                    p['field']: field_count + 1
                })
        else:
            total.add(p['row'])
            sample.append({
                'row': p['row'],
                'field': '',
                'value': p['missing'],
                'message': p['message']
            })

    #update data tag
    error_rows_index = itertools.imap(lambda x: x-1, total)
    for i in error_rows_index:
        raw_data.tag[i] = 101

    raw_data.to_csv(path, sep="\t", chunksize=100000, index=False, encoding="utf-8")

    total_problems = {'column_total': column_total, 'total': len(total), 'sample': sample}
    end = time.clock()

    spent = end - start

    robot = ADMonitorAuditRobot(count, total_problems, spent)

    robot.report()



if __name__ == '__main__':
    main('util/ad.csv')
