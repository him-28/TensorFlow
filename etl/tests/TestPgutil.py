#encoding=utf-8

import sys
import unittest

sys.path.insert(1, '../..')

from etl.util.pgutil import DBUtils


class TestPGUtil(unittest.TestCase):

    def setUp(self):
        self.insert_sql = "insert into \"AD_Facts_By_Day\" (date_id) VALUES (10001)"
        self.error_insert_sql = "insert into \"AD_Facts_By_Day\" (date_id) VALUES (20001,20001)"
        self.bulkInsert_sql = "insert into \"AD_Facts_By_Day\" (date_id,click) VALUES ( %s, %s )"
        self.error_bulInsert_sql="insert into \"AD_Facts_By_Day\" (date_id,click) VALUES ( %s,%s,%s )"
        self.fetchone_sql = "select date_id from \"AD_Facts_By_Day\" where date_id=10001"
        self.error_fetchone_sql="select date_id from \"AD_Facts_By_Dayxx\" where date_id=10001"
        self.fetchall_sql = "select date_id from \"AD_Facts_By_Day\" where date_id=10001"
        self.error_fetchall_sql="select date_id from \"AD_Facts_By_Dayxx\" where date_id=10001"

    def test0_insert(self):
        DBUtils.insert(self.insert_sql)
        result=DBUtils.fetchone(self.fetchone_sql)
        self.assertIsNotNone(result, "result is  null")
        self.assertEqual(result, 10001, "not equal")
        #测试异常情况
        DBUtils.insert(self.error_insert_sql)
        

    def test1_bulkInsert(self):
        vals=[(10002,12),(10003,13),(10004,14)]
        DBUtils.bulkInsert(self.bulkInsert_sql,vals)
        sql = "select date_id from \"AD_Facts_By_Day\" where date_id=%d"
        for t in vals:
            result = DBUtils.fetchone(sql % t[0])
            self.assertIsNotNone(result, "result is null")
            self.assertEqual(result, t[0], "not equal")
        #测试异常
        DBUtils.bulkInsert(self.error_bulInsert_sql)
    def test2_fetchone(self):
        DBUtils.insert(self.insert_sql)
        row=DBUtils.fetchone(self.fetchone_sql)
        self.assertIsNotNone(row, "result is null")
        self.assertEqual(row, 10001, "not equal")
        #测试异常
        DBUtils.fetchone(self.error_fetchone_sql)

    def test3_fetchall(self):
        DBUtils.insert(self.insert_sql)
        rows = DBUtils.fetchall(self.fetchall_sql)
        self.assertIsNotNone(rows, "result is null")
        self.assertEqual(rows[0][0], 10001, "not equal")
        #测试异常
        DBUtils.fetchall(self.error_fetchall_sql)
            
    def tearDown(self):
        clear_sql="delete  from \"AD_Facts_By_Day\" where date_id=%d"
        vals=[(10002),(10003),(10004)]
        DBUtils.insert(clear_sql%10001)
        for t in vals:
            DBUtils.insert(clear_sql%t)
        
if __name__ == '__main__':
    unittest.main()
