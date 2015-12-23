# encoding: utf-8
'''
etl.util.reportutil -- report ETL result to media

etl.util.reportutil is a util class to report result from
the ETL to email/bearychat with txt/img/pdf/html formats

It defines class Reportor

@author:     wennu

@copyright:  2015 http://www.mgtv.com. All rights reserved.

@license:    no licenses

@contact:    wennu@e.hunantv.com
'''
import os
import yaml

import traceback
import trml2pdf
import preppy
import reportlab.lib.styles
from reportlab.pdfbase import pdfmetrics, ttfonts
from reportlab.lib.fonts import addMapping

from etl.conf.settings import LOGGER as LOG
from etl.conf.settings import CURRENT_ENV
from etl.util import bearychat as bc
from etl.report.reporter import get_pf_name

ENV_CONF = yaml.load(file("conf/platform_monitor_config.yml"))
SCNF = ENV_CONF[CURRENT_ENV]["store"]

REPORT_CHANNEL = SCNF["bearychat_channel"]
REPORT_CHANNEL_DAY = SCNF["bearychat_channel_day"]


class PDFUtils(object):
    """ PDF 生成工具类  """
    def __init__(self, data_info, dtime):
        """pdf util"""
        font_dir = "./report/pdf/"
        super(PDFUtils, self).__init__()
        self.static_dir = "./report/pdf/mglogo.png"
        self.data_info = data_info
        self.dtime = dtime
        try:
            # 注册字体
            pdfmetrics.registerFont(ttfonts.TTFont('song', \
                                                   os.path.join(font_dir, 'MSYHBD.TTF')))
            # 宋体
            pdfmetrics.registerFont(ttfonts.TTFont('msyh', \
                                                   os.path.join(font_dir, 'MSYH.TTF')))
            # 注册宋体粗体字体
            pdfmetrics.registerFont(ttfonts.TTFont('song_b', \
                                                   os.path.join(font_dir, 'STZHONGS.TTF')))
        except:
            LOG.error(traceback.format_exc())

        addMapping('song', 0, 0, 'song')  # normal
        addMapping('song', 0, 1, 'song')  # italic
        addMapping('song', 1, 1, 'song_b')  # bold, italic
        addMapping('song', 1, 0, 'song_b')
        addMapping('songs', 0, 0, 'songs')  # bold

        # 设置自动换行
        reportlab.lib.styles.ParagraphStyle.defaults['wordWrap'] = "CJK"

    def create_pdf(self, templ, save_file):
        """从二进制流中创建PDF并返回
        @param templ 需要渲染的XML文件地址（全路径）
        @param save_file PDF文件保存的地址（全路径）
        """
        # 读取模板文件
        template = preppy.getModule(templ)
        # 渲染模板文件
        new_dict = {}
        for the_pf, datas in self.data_info["details"].iteritems():
            new_dict.update({get_pf_name(the_pf):datas})
        namespace = {
            'STATIC_DIR': self.static_dir,
            'timenow': self.dtime,
            #"display_poss": int(self.data_info["display_poss"]),
            "display_sale": int(self.data_info["display_sale"]),
            "impression": int(self.data_info["impression"]),
            "impression_end": int(self.data_info["impression_end"]),
            "display_sale": int(self.data_info["display_sale"]),
            "click": click
            }
        # 渲染PDF页面
        rml = template.getOutput(namespace)
        # 生成PDF
        pdf = trml2pdf.parseString(rml)
        # 保存PDF
        open(save_file, 'wb').write(pdf)
        return True

class PlatformReportor:
    '''Report ETL result'''
    def __init__(self):
        pass

    def report_hour(self, data_date, infos):
        '''Report ETL result data in text format
        '''
        LOG.info("report infos : %s " % infos)
        text = "小时管理后台统计【%s】" % (data_date.strftime('%Y%m%d %H'))
        at_title = "【%s时数据汇总报告】" % (data_date.strftime('%H'))
        msg = "文件名：%s，文件大小：%s，统计耗时：%ss\n【投放】：%s，【开始播放】： %s， 【点击数】：%s" % \
            (infos["file_name"], infos["file_size"], infos["spend_time"],  infos["display_sale"], infos["impression"], infos["click"])
        bc.new_send_message(text=text, at_title=at_title, \
                            channel=REPORT_CHANNEL , at_text=msg)

    def report_day(self, data_date, infos):
        '''Report ETL result data in text format
        '''
        LOG.info("report infos : %s " % infos)
        text = "天管理后台统计【%s】" % (data_date.strftime('%Y%m%d'))
        at_title = "【今日00时-23时数据汇总报告】"
        msg = "统计耗时：%ss\n【投放】：%s，【开始播放】： %s， 【点击数】：%s" % \
            (infos["spend_time"], infos["display_sale"], infos["impression"], infos["click"])
        bc.new_send_message(text=text, at_title=at_title, \
                            channel=REPORT_CHANNEL_DAY , at_text=msg)
