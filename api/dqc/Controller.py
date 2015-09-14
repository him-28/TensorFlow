#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
'''


class controller(): 
    def check_param(self,params,ctype):
        print "check_params..."
        #校验参数是否是合适的json串
        # ids 与name不可同时存在
        # 维度之间的约束
        if not params.has_key("dateRange"):
            return "日期范围查询条件不能为空"
        if params.has_key("name") and params.has_key("ids") and params["ids"] and params["name"].strip():
            return "查询条件不能同时为ids和订单名称"
        return "success"
        
    def get_param(self):
        return {}
    
    def get_query_sql(self,params):
        return ""
