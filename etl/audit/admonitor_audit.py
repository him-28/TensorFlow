#encoding=utf-8
# author: martin

import sys
import pandas as pd
import numpy as np

from datetime import datetime

from etl.util.csvvalidator import CSVValidator, enumeration, number_range_inclusive,\
        unsignedint_inclusive, match_pattern, str_len, str_len_range, \
        write_problems, emit_problems, datetime_string, RecordError
from etl.util.bearychat import new_send_message

def admonitor_csv_validator():
    """Create AD Monitor CSV validator for data"""
    field_names = (
            'ip',
            'province',
            'city',
            'ad_event_type',
            'url',
            'video_id',
            'playlist_id',
            'board_id',
            'request_res',
            'ad_list',
            'time_delay',
            'request_str',
            'slot_id',
            'compaign_id',
            'creator_id',
            'video_play_time',
            'order',
            'group_id',
            'play_event',
            'pf',
            'device_id',
            'uid',
            'net',
            'os',
            'manufacturer',
            'model',
            'app',
            'timestamp',
            'session_id',
            'tag'
            )

    validator = CSVValidator(field_names)
    
    #basic header and record length check
    validator.add_header_check('EX1', 'bad header')
    validator.add_record_length_check('EX2', 'bad header')

    #value check

    validator.add_value_check('ip', unsignedint_inclusive,
                              'EX3', 'invalid city')
    validator.add_value_check('province', unsignedint_inclusive,
                              'EX4', 'invalid city')
    validator.add_value_check('city', unsignedint_inclusive,
                              'EX5', 'invalid city')
    validator.add_value_check('ad_event_type', enumeration('e', 'p'),
                              'EX6', 'invalid ad_event_type')
    validator.add_value_check('url', match_pattern(r"^(http)$"),
                              'EX7', 'invalid url')
    validator.add_value_check('video_id', unsignedint_inclusive,
                              'EX8', 'invalid video_id')
    validator.add_value_check('playlist_id', unsignedint_inclusive,
                              'EX9', 'invalid playlist_id')
    validator.add_value_check('board_id', unsignedint_inclusive,
                              'EX10', 'invalid board_id')
    validator.add_value_check('time_delay', unsignedint_inclusive,
                              'EX11', 'invalid time_delay')
    validator.add_value_check('request_str', match_pattern(r"^(http)$"),
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
    validator.add_value_check('pf', enumeration('000100', '2'),
                              'EX20', 'invalid pf')
    validator.add_value_check('device_id', str_len_range(40, 48),
                              'EX21', 'invalid device_id')
    validator.add_value_check('net', enumeration('WIFI', 'wifi', '3g', '4g', '2g', u'蜂窝'),
                              'EX22', 'invalid net')
    validator.add_value_check('session_id', str_len(36),
                              'EX23', 'invalid uuid')
    validator.add_value_check('tag', number_range_inclusive(0,1000, int),
                              'EX24', 'invalid tag')

    return validator



fmt = ""
class ADMonitorAuditRobot(object):
    def write(self, content, color='yellow'):
        #fmt = d
        title = u"小金汇报(android 手机)"
        channel = u'广告-数据'
        m_title = u'数据审计完成 %s' % str(datetime.now())
        if color == 'yellow':
            new_send_message(title , True, channel, m_title, content, "#ffa500")
        else:
            new_send_message(title , True, channel, m_title, content, "#8B0000")

def main(path):
    from pdb import set_trace as st
    #df = pd.read_csv(path, sep="\t", chunksize=10000000, index_col=False, encoding="utf-8", dtype={'pf': np.str})
    err_problems = []
    #for chunk in df:
    import csv
    with open(path, 'r') as f:
        data = csv.reader(f, delimiter='\t')
        validator = admonitor_csv_validator()
        problems = validator.validate(data,
                                    summarize= False,
                                    report_unexpected_exceptions=False,
                                    context={'file': True})
        #err_problems.append(problems)
        #write_problems(problems,
        #            sys.stdout,
        #            summarize= False,
        #            limit=0)

        robot = ADMonitorAuditRobot()

        emit_problems(problems,
                    robot.write,
                    limit=0)

if __name__ == '__main__':
    main('util/ad.csv')
