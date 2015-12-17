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

ENV_CONF = yaml.load(file("conf/inventory_monitor_config.yml"))
SCNF = ENV_CONF[CURRENT_ENV]["store"]

REPORT_CHANNEL = SCNF["bearychat_channel"]


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
            "display_poss": int(self.data_info["display_poss"]),
            "display_sale": int(self.data_info["display_sale"]),
            "sum_list": new_dict
            }
        # 渲染PDF页面
        rml = template.getOutput(namespace)
        # 生成PDF
        pdf = trml2pdf.parseString(rml)
        # 保存PDF
        open(save_file, 'wb').write(pdf)
        return True

class InventoryReportor:
    '''Report ETL result'''
    def __init__(self):
        pass

    def report_pdf(self, data_info, dtime):
        '''生成 PDF 报告'''
        LOG.info("report pdf...")
        pdf_util = PDFUtils(data_info, dtime)
        # 模板页面地址
        year = dtime[0:4]
        month = dtime[4:6]
        prefix_path = os.path.join("/data6/inventory/", year, month)
        if not os.path.exists(prefix_path):
            os.makedirs(prefix_path)
        temp_path = './report/pdf/inventory_report.prep'
        pdf_path = os.path.join(prefix_path, 'inventory_report_%s.pdf' % dtime)
        LOG.info("create PDF at %s", pdf_path)
        # 如果PDF不存在则重新生成
        if  os.path.exists(pdf_path):
            os.remove(pdf_path)
        pdf_util.create_pdf(temp_path, pdf_path)


    def report_hour(self, data_date, infos, channel=REPORT_CHANNEL):
        '''Report ETL result data in text format'''
        LOG.info("report infos : %s " % infos)
        text = "小时库存统计【%s】" % (data_date.strftime('%Y%m%d %H'))
        at_title = "【汇总报告】"
        msg = "文件大小：%s，统计耗时：%ss\n【展示机会】： %s， 【投放】：%s" % \
            (infos["file_size"], infos["spend_time"], infos["display_poss"], infos["display_sale"])
        bc.new_send_message(text=text, at_title=at_title, \
                            channel=channel , at_text=msg)

        details = infos["details"]
        if details:
            for the_pf, datas in details.iteritems():
                text = get_pf_name(the_pf)
                at_title = "【%s】小时数据" % (data_date.strftime('%Y%m%d %H'))
                msg = "【展示机会】： %s， 【投放】：%s" % \
                    (datas["display_poss"], datas["display_sale"])
                bc.new_send_message(text=text, at_title=at_title, \
                                    channel=channel , at_text=msg)

    def report_day(self, data_date, infos, channel=REPORT_CHANNEL):
        '''Report ETL result data in text format
        '''
        LOG.info("report infos : %s " % infos)
        text = "天库存统计【%s】" % (data_date.strftime('%Y%m%d'))
        at_title = "【汇总报告】"
        msg = "统计耗时：%ss\n【展示机会】： %s， 【投放】：%s" % \
            (infos["spend_time"], infos["display_poss"], infos["display_sale"])
        bc.new_send_message(text=text, at_title=at_title, \
                            channel=channel , at_text=msg)

        details = infos["details"]
        if details:
            for pf, datas in details.iteritems():
                text = get_pf_name(pf)
                at_title = "【%s】天数据" % (data_date.strftime('%Y%m%d'))
                msg = "【展示机会】： %s， 【投放】：%s" % \
                    (datas["display_poss"], datas["display_sale"])
                bc.new_send_message(text=text, at_title=at_title, \
                                    channel=channel , at_text=msg)
