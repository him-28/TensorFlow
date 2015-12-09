#!/usr/bin/python
# -*- coding: utf-8 -*-



import os
import preppy
import logging
import traceback
import trml2pdf
from datetime import datetime

import reportlab.lib.styles
from reportlab.pdfbase import pdfmetrics, ttfonts
from reportlab.lib.fonts import addMapping

from etl.conf.settings import LOGGER, Config
operation_logger = logging.getLogger('operation')
bug_log = logging.getLogger('bug')


class PDFUtils(object):

    """ PDF 生成工具类  """


    def __init__(self,
                    sum_list,
                    ad_data,
                    dtime, 
                    font_dir="./report/pdf/"):
        """ 

        @param font_dir 需要注册的字体文件目录
        @param static_dir 静态文件地址目录 
        """

        super(PDFUtils, self).__init__()
        self.STATIC_DIR = "./report/pdf/mglogo.png"
        self.sum_list = sum_list
        self.ad_data = ad_data
        self.dtime = dtime
        try:
            # 注册字体
            pdfmetrics.registerFont(ttfonts.TTFont('song', os.path.join(font_dir, 'MSYHBD.TTF')))
            #宋体
            pdfmetrics.registerFont(ttfonts.TTFont('msyh', os.path.join(font_dir, 'MSYH.TTF')))
            # 注册宋体粗体字体
            pdfmetrics.registerFont(ttfonts.TTFont('song_b', os.path.join(font_dir, 'STZHONGS.TTF')))
        except:
            LOGGER.error(traceback.format_exc())

        addMapping('song', 0, 0, 'song')     # normal
        addMapping('song', 0, 1, 'song')     # italic
        addMapping('song', 1, 1, 'song_b')     # bold, italic
        addMapping('song', 1, 0, 'song_b')
        addMapping('songs', 0, 0, 'songs')     # bold

        # 设置自动换行
        reportlab.lib.styles.ParagraphStyle.defaults['wordWrap'] = "CJK"


    def create_pdf(self,  templ, save_file):
        """从二进制流中创建PDF并返回

        @param templ 需要渲染的XML文件地址（全路径）
        @param save_file PDF文件保存的地址（全路径）
        """
        # 读取模板文件
        template = preppy.getModule(templ)
        # 渲染模板文件
        namespace = {
            'STATIC_DIR': self.STATIC_DIR,
            'timenow': self.dtime,
            "ad_data": self.ad_data,
            "sum_list": self.sum_list
            }
        # 渲染PDF页面
        rml = template.getOutput(namespace)
        # 生成PDF
        pdf =  trml2pdf.parseString(rml)
        # 保存PDF
        open(save_file,'wb').write(pdf)
        return True
PF_SNAME = {
        "000000":"PC Web", "000100":"PC Mac Client", \
        "000101":"PC Windows Client", "010000":" iPad App", \
        "010001":"Android Pad App", "010100":"iPhone App", \
        "010101":"Android Phone App", "010200":"Mobile Web Site", \
        "010201":"Pad Web Site", "020000":"OTT"
}
def  generate_pdf_report(ad_data,sum_data,dtime):
    new_ad_data = {}
    if ad_data:
        for pf_id,values in ad_data.items():
            pf_name = PF_SNAME.get(pf_id)
            if pf_name is None:
                pf_name = "未知"
            new_ad_data[pf_name] = values
    
    new_sum_data = {}
    if sum_data:
        for pf_id,values in sum_data.items():
            pf_name = PF_SNAME.get(pf_id)
            if pf_name is None:
                pf_name = "未知"
            new_sum_data[pf_name] = values
            
    pu = PDFUtils(new_sum_data,new_ad_data,dtime)
    # 模板页面地址
    year = dtime[0:4]
    month = dtime[4:6]
    prefix_path = os.path.join(Config["data_prefix"],year,month)
    if not os.path.exists(prefix_path):
        os.makedirs(prefix_path)
    temp_path = './report/pdf/pre_test.prep'
    pdf_path = os.path.join(prefix_path,'ad_report_%s.pdf'%dtime)
    # 如果PDF不存在则重新生成
    if  os.path.exists(pdf_path):
        os.remove(pdf_path) 
    pu.create_pdf(temp_path, pdf_path)

if __name__ == '__main__':
    
    ad_data = { "000000":{"4580":[{
                              "前贴":[
                                    ['1','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ['2','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ['3','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ]},
                              {"暂停":[
                                    ['1','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ]
                              }]},
            "010101":{"4580":[{
                              "前贴":[
                                    ['1','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ['2','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ['3','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ]},
                              {"暂停":[
                                    ['1','40000','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ]
                              }]},
            "010100":{"4580":[{
                              "前贴":[
                                    ['1','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ['2','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ['3','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ]},
                              {"暂停":[
                                    ['1','40000','40000','40000','40000','40000','80%',"0.23%"],
                                    ]
                              }]}
           }
    sum_list = {"000000":['40000','40000','40000','40000','40000','80%',"0.23%"],
                "010101":['40000','40000','40000','40000','40000','80%',"0.23%"],
                "010100":['40000','40000','40000','40000','40000','80%',"0.23%"]}
    
    tn = datetime.now()
    timenow = datetime.strftime(tn,"%Y%m%d")
    generate_pdf_report(ad_data,sum_list,timenow)
    print 'create pdf done'