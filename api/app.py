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
import json
DIR_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
import sys
sys.path.append(DIR_PATH)
sys.path.append(os.path.join(DIR_PATH,'./util'))
from api_utils import get_property
from db_utils import get_resultFromDb

class HelloHandler(tornado.web.RequestHandler):
	def get(self):
		self.write("Hello world")

class OrderReportHandler(Handler,tornado.web.RequestHandler):
	
	def self_check_param(self,params):
		#维度检查
		#TODO {"dateRange":{"start":"2015-09-01","end":"yyyy-mm-dd"},"ids":["id1","id2"],"name":" ","priDim":{"date":"1"},"subDim":{"region":"1"},"page":"2","pageNum":"10"}
		if params.has_key("priDim") and params["priDim"] and params.has_key("subDim") and params["subDim"]:
			pri_dim=params["priDim"]
			sub_dim=params["subDim"]
			sub_dim_check=get_property("dim_check",pri_dim)
			if not sub_dim_check or not sub_dim in sub_dim_check:
				return "当前主维度"+pri_dim+"下，不允许次级维度"+sub_dim
		return "success"
	def get_query_sql(self,params):
		"""根据请求参数，组织查询sql"""
		dic=json_decode(params)
		#只有date 和time维度
		start_date=dic["dateRange"]["start"]
		end_date=dic["dateRange"]["end"]
		page=int(dic["page"])
		pageNum=int(dic["pageNum"])
		
		#test sql
		sql="select reqs_total,impressions_start_total,impressions_finish_total,click,hit_total from ad_facts_by_hour afh limit "+str(pageNum)+" offset "+str((page-1)*pageNum)
		return sql
	def get_response(self,rows_result):
		"""根据数据库的查询结果，组织响应报文"""
		return "{\"returnCode\":\"200\",\"message\":\"success\"}"
	def get(self):
		pass
	
	def post(self):
        #校验订单报告请求参数
		params=self.request.body
		result=self.check_param(params,"order")
		if result != "success":
			self.write("{\"returnCode\":\"201\",\"message\":\""+result+"\"}")
			pass
		sql=self.get_query_sql(params)
		rows=get_resultFromDb(sql)
		res=self.get_response(rows)
		self.write(res)

class VersionHandler(tornado.web.RequestHandler):
	def get(self):
		version=get_property("version","version")
		if version:
			json.dumps({"version":version})
		
if __name__ == "__main__":
	application = tornado.web.Application([
		(r'/', HelloHandler),
		(r'/orderReport', OrderReportHandler)
		(r'/version', VersionHandler),
	])
   	application.listen(settings.port)
   	tornado.ioloop.IOLoop.instance().start()
	
    
