#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
'''
import tornado.ioloop 
import tornado.web 
from orderController import orderController


application = tornado.web.Application([
(r"/report_test1", orderController),
]) 
if __name__ == "__main__":
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
    