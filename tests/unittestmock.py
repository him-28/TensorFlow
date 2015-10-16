# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
#    +Author wennu
#    +Email wenu@e.hunantv.com


"""
use Mock to test dqc program 
"""

import os.path
import sys
DIR_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
sys.path.append(DIR_PATH)
sys.path.append(os.path.join(DIR_PATH,'../api'))
#print sys.path

from mock import Mock, call,  patch
from app import HelloHandler, OrderReportHandler, VersionHandler
import tempfile
import unittest
import mock

class HelloTestCase(unittest.TestCase):
   
    def setUp(self):
	print "Create a API case:setup_begin"

    def tearDown(self):
	print "Destroying a API case:teardown_begin"
   
    @mock.patch.object(HelloHandler, 'get')
    def test_hellohandler(self, mock_get):
	# wirte a "hello world"
	hellohandler = HelloHandler()
	hellohandler.get()
	hellohandler.assert_any_call()
	
	mock_get.get()
	mock_get.get.assert_any_call()
	mock_get.get('get any para')
	mock_get.get.assert_called_with('get any para')

class OrderReportTestCase(unittest.TestCase):
    
    @mock.patch.object(OrderReportHandler, 'self_check_param')
    @mock.patch.object(OrderReportHandler, 'get_query_sql')
    @mock.patch.object(OrderReportHandler, 'get_response')
    @mock.patch.object(OrderReportHandler, 'get')
    @mock.patch.object(OrderReportHandler, 'post')

    def test_orderreporthandler(self, mock_post, mock_get, mock_get_response, mock_get_query_sql, mock_self_check_param):
	#order report request check
	OrderClass = OrderReportHandler()
	OrderReport = Mock(OrderClass, return_value = 'para')

	# Mock
	print OrderReport

	# def post
	mock_post.post()
	mock_post.post.assert_called_once_with()
	mock_post.post()
	#mock_post.post.assert_called_once_with()
	
	# MagicMock
	print mock_post.post.result

	mock_get.get()
	mock_get.get.assert_any_call()

	# object call
	OrderReport.post()
	OrderReport.get()
	OrderReport.get_response('response')
	OrderReport.self_check_param('check para')
	OrderReport.get_query_sql('sql')

	OrderReportCalls = [call.post(), call.get(), call.get_response('response'), call.get_query_sql('sql'), call.self_check_param('check para')]
	OrderReport.assert_has_calls(OrderReportCalls, any_order = True)

	OrderReport()
	OrderReport('paraA')
	OrderReport('paraB')
	
	# parameters call
	print 'args list'
	print OrderReport.call_args_list
	
	# factory call
	print 'factory call'
	print OrderReport.called
	
	# method call
	print 'method call'
	print OrderReport.method_calls
	
	# object call
	print 'object call'
	print OrderReport.mock_calls


	
if __name__ == '__main__':
    
    unittest.main()
