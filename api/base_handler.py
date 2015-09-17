#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
'''
from tornado.escape import json_decode, json_encode
import settings

class Handler(): 
    def check_param(self,params,ctype):
        print "check_params..."
        #校验参数是否是合适的json串
        # ids 与name不可同时存在
        # 维度之间的约束
        try:
        	dic_params=json_decode(params)
        except:
        	return "不是合法的json字符串"
        if not dic_params.has_key("dateRange"):
            return "日期范围查询条件不能为空"
        if dic_params.has_key("name") and dic_params.has_key("ids") and dic_params["ids"] and dic_params["name"].strip():
            return "查询条件不能同时为ids和名称"
        #维度之间的约束检查
        #{"dateRange":{"start":"2015-09-01","end":"yyyy-mm-dd"},"ids":["id1","id2"],"name":" ","priDim":{"date":"1"},"subDim":{"region":"1"},"page":"2","pageNum":"10"}
        resu=self.self_check_param(dic_params)
        return resu
 
    def get_param(self):
        return {}
    
    def get_query_sql(self,params):
        return ""
    
    def self_check_param(self,params):
        return "success"
