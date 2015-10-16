#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
'''
from Controller import controller
import tornado.web
from tornado.escape import json_decode, json_encode
from orderDao import orderDAO
import datetime

class orderController(controller,tornado.web.RequestHandler):

    def get(self): 
        self.check_param()
        self.write("Hello, world")
        
    def post(self):
        params=self.request.body
        #解析body为字典对象
        dic=json_decode(params)
        #校验订单报告请求参数
        result=self.check_param(dic,"order")
        if result != "success":
            self.write("{\"returnCode\":\"201\",\"message\":\""+result+"\"}")
            return
        sql=self.get_query_sql(params)
        if dic.has_key("dateRange"):
            date_range=dic["dateRange"]
        orderd=orderDAO()    
        rows=orderd.select_all_by_date(date_range)
        if rows:
            result={"returnCode":"200","message":"success"}
            result["result"]={}
            for item in rows:
                if not result["result"].has_key(item[0]):
                    result["result"][item[0]]={}
                result["result"][item[0]]["msg1"]=item[1]
                result["result"][item[0]]["msg2"]=datetime.datetime.strftime(item[2],'%Y-%m-%d')
        str_result=json_encode(result)
        self.write(str_result)
        #取出参数中的维度信息
#         for i in dic:
#             print i,'-',dic[i]
#             if i == "dateRange":
#                 for x in dic[i]:
#                     print x,"-",dic[i][x]
#             if i == "ids":
#                 for j in dic[i]:
#                     print j
#         params=self.get_argument("data")
#         print params

