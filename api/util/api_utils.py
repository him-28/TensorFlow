#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
'''
import ConfigParser
import json

config_file_path="../api_config.ini"
def get_property(options,items):
    cf = ConfigParser.ConfigParser()
    cf.read(config_file_path)
    s = cf.sections()
#    print 'section:', s
#    o = cf.options("dim_check")
#    print 'options:', o
#    v = cf.items("dim_check")
#    print 'property:', v
#    date_limit = cf.get("dim_check", "pri_date_4")
#    print date_limit
#    if "os1" in date_limit:
#        print "true"
    try:
        if options in s and  items in cf.options(options):
            return cf.get(options, items)
        else: return "false"
    except:
        return "not found item:"+items
        
if __name__ == "__main__":
    get_property("", "")
