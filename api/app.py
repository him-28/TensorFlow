#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: Administrator
'''
import tornado.ioloop 
import tornado.web 

import settings

class HelloHandler(tornado.web.RequestHandler):
	def get(self):
		self.write("Hello world")

class OrderReportHandler(tornado.web.RequestHandler):
	def get(self):
		pass

if __name__ == "__main__":
	application = tornado.web.Application([
		(r'/', HelloHandler)
	])
    application.listen(settings.port)
    tornado.ioloop.IOLoop.instance().start()
    
