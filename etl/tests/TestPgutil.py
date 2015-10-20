#encoding=utf-8

import sys
import unittest

sys.path.insert(1, '../..')

from etl.utils.pgutil import DBUtils


class TestPGUtil(unittest.TestCase):

    def setUp(self):
        self.insert_sql = ""
        self.bulkInsert_sql = ""
        self.fetchone_sql = ""
        self.fetchall_sql = ""

    def test0_insert(self):
        pass

    def test1_bulkInsert(self):
        pass

    def test2_fetchone(self):
        pass

    def test3_fetchall(self):
        pass
        
