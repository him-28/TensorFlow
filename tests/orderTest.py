import unittest
import os,sys
import tornado
DIR_PATH = os.path.abspath(os.path.dirname(os.path.realpath(__file__)))
import sys
sys.path.append(DIR_PATH)
sys.path.append(os.path.join(DIR_PATH,'../api'))
from app import OrderReportHandler



def addNum(a, b):
    return a+b

class OrderTest(unittest.TestCase):

    def setUp(self):

        print os.path.dirname(os.path.abspath(__file__))
        print "OrderTest:setUp_:begin"

        testName = self.id().split(".")
        testName = testName[2]
        print testName

    def tearDown(self):
        print "OrderTest:tearDown_:begin"
        print ""

    def testA(self):
        self.assertEqual(3, addNum(1,1))

    def testB_self_check_param(self):
        orderReportHandler = OrderReportHandler()
        testParam = {"dateRange":{"start":"2015-09-01","end":"yyyy-mm-dd"},"ids":["id1","id2"],"name":" ","priDim":{"date":"1"},"subDim":{"region":"1"},"page":"2","pageNum":"10"}
        self.assertEqual("success", orderReportHandler.self_check_param(testParam))
        

if __name__ == '__main__':
	unittest.main()
    
