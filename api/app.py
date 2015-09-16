#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
'''
import tornado.ioloop 
import tornado.web 
from tornado.escape import json_decode, json_encode
import settings
from base_handler import Handler
import os
DIR_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))

import sys
sys.path.append(DIR_PATH)
sys.path.append(os.path.join(DIR_PATH,'./order'))
from order_request_handler import OrderRequestHandler

orderRequestHandler=OrderRequestHandler()

class HelloHandler(tornado.web.RequestHandler):
	def get(self):
		self.write("Hello world")

class OrderReportHandler(Handler,tornado.web.RequestHandler):
	
	def self_check_param(self,params):
		return "not successs"
	
	def get(self):
		pass
	
	def post(self):
        #解析body为字典对象
        #dic=json_decode(params)
        #校验订单报告请求参数
		params=self.request.body
		result=self.check_param(params,"order")
		print result
# 		result=orderRequestHandler.check_param(ss,"order")

#         print result
#         if result != "success":
#             self.write("{\"returnCode\":\"201\",\"message\":\""+result+"\"}")
#             return
#         sql=self.get_query_sql(params)
#         if dic.has_key("dateRange"):
#             date_range=dic["dateRange"]
#         orderd=orderDAO()    
#         rows=orderd.select_all_by_date(date_range)
#         if rows:
#             result={"returnCode":"200","message":"success"}
#             result["result"]={}
#             for item in rows:
#                 if not result["result"].has_key(item[0]):
#                     result["result"][item[0]]={}
#                 result["result"][item[0]]["msg1"]=item[1]
#                 result["result"][item[0]]["msg2"]=datetime.datetime.strftime(item[2],'%Y-%m-%d')
#         str_result=json_encode(result)
#         self.write(str_result)
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


if __name__ == "__main__":
	application = tornado.web.Application([
		(r'/', HelloHandler),
		(r'/orderReport', OrderReportHandler)
	])
   	application.listen(settings.port)
   	tornado.ioloop.IOLoop.instance().start()
	
    
