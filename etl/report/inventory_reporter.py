# encoding: utf-8
'''
etl.util.reportutil -- report ETL result to media

etl.util.reportutil is a util class to report result from
the ETL to email/bearychat with txt/img/pdf/html formats

It defines class Reportor

@author:     dico.ding

@copyright:  2015 http://www.mgtv.com. All rights reserved.

@license:    no licenses

@contact:    dingzheng@imgo.tv
'''

import yaml

from etl.conf.settings import LOGGER as LOG
from etl.conf.settings import CURRENT_ENV
from etl.util import bearychat as bc
from etl.report.reporter import get_pf_name

ENV_CONF = yaml.load(file("conf/inventory_monitor_config.yml"))
SCNF = ENV_CONF[CURRENT_ENV]["store"]

REPORT_CHANNEL = SCNF["bearychat_channel"]

class InventoryReportor:
    '''Report ETL result'''
    def __init__(self):
        pass

    def report_hour(self, data_date, infos):
        '''Report ETL result data in text format
        '''
        LOG.info("report infos : %s " % infos)
        text = "小时库存统计【%s】" % (data_date.strftime('%Y%m%d %H'))
        at_title = "【汇总报告】"
        msg = "文件名：%s，文件大小：%s，统计耗时：%ss\n【展示机会】： %s， 【投放】：%s" % \
            (infos["file_name"], infos["file_size"], infos["spend_time"], infos["display_poss"], infos["display_sale"])
        bc.new_send_message(text=text, at_title=at_title, \
                            channel=REPORT_CHANNEL , at_text=msg)

        details = infos["details"]
        if details:
            for pf, datas in details.iteritems():
                text = get_pf_name(pf)
                at_title = "【%s】小时数据" % (data_date.strftime('%Y%m%d %H'))
                msg = "【展示机会】： %s， 【投放】：%s" % \
                    (datas["display_poss"], datas["display_sale"])
                bc.new_send_message(text=text, at_title=at_title, \
                                    channel=REPORT_CHANNEL , at_text=msg)

    def report_day(self, data_date, infos):
        '''Report ETL result data in text format
        '''
        LOG.info("report infos : %s " % infos)
        text = "天库存统计【%s】" % (data_date.strftime('%Y%m%d'))
        at_title = "【汇总报告】"
        msg = "统计耗时：%ss\n【展示机会】： %s， 【投放】：%s" % \
            (infos["spend_time"], infos["display_poss"], infos["display_sale"])
        bc.new_send_message(text=text, at_title=at_title, \
                            channel=REPORT_CHANNEL , at_text=msg)

        details = infos["details"]
        if details:
            for pf, datas in details.iteritems():
                text = get_pf_name(pf)
                at_title = "【%s】天数据" % (data_date.strftime('%Y%m%d'))
                msg = "【展示机会】： %s， 【投放】：%s" % \
                    (datas["display_poss"], datas["display_sale"])
                bc.new_send_message(text=text, at_title=at_title, \
                                    channel=REPORT_CHANNEL , at_text=msg)
