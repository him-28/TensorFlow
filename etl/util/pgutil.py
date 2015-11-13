#encoding=utf-8

import os
import psycopg2

from psycopg2.pool import ThreadedConnectionPool

from etl.conf.settings import LOGGER
from etl.conf.settings import Config as CONFIG


class DBConfig:
    
    @staticmethod
    def defaultConfig():
        return {
            'database':  'postgres',
            'user': 'postgres',
            'password': 'adpg@150909',
            'host': '10.100.5.80',
            'port': '11921',
            'minconn': 5,
            'maxconn': 50
        }

    @staticmethod
    def Config():
        config = DBConfig.defaultConfig()
        config.update({
            'database': CONFIG['database']['db_name'],
            'user': CONFIG['database']['user'],
            'password': CONFIG['database']['password'],
            'host': CONFIG['database']['host'],
            'port': CONFIG['database']['port'],
            'minconn': CONFIG['database']['minconn'],
            'maxconn': CONFIG['database']['maxconn']
            })
        return config


config = DBConfig.Config()


class SafePoolManager:
    """
    pool = SafePoolManager(1, 10, "host='127.0.0.1' port=12099")
    """

    def __init__(self, *args, **kwargs):
        self.last_seen_process_id = os.getpid()
        self.args = args
        self.kwargs = kwargs
        self._pool = None

    def _init(self):
        self._pool = ThreadedConnectionPool(self.args[0],self.args[1],
                                            database=config.get('database'),
                                            user=config.get('user'),
                                             password=config.get('password'), 
                                             host=config.get('host'), 
                                             port=config.get('port'))

    def getconn(self):
        current_pid = os.getpid()
        if not (current_pid == self.last_seen_process_id):
            self._init()
            LOGGER.info("New id is %s, old id was %s" % (current_pid, self.last_seen_process_id))
            self.last_seen_process_id = current_pid
        if self._pool is None:
            self._init()
        return self._pool.getconn()
    
pool = SafePoolManager(config['minconn'], config['maxconn'])
class DBUtils:
    """
    insert: DBUtils.insert('insert into [table] values(1,2,3)')
    bulkInsert: DBUtils.bulkInsert('insert into [table] values(%d, %d, %d)', [(1,2,3), (4,5,6)])
    """
    @staticmethod
    def insert(sql):
        try:
            conn = pool.getconn()
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        except psycopg2.DatabaseError, e:
            LOGGER.error('pg insert error: %s' % e)
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                conn.close()

    @staticmethod
    def bulkInsert(sql,vals=[],dbconn=None,commit=True,bulk_size=1000):
        '''
        bulkInsert: DBUtils.bulkInsert('insert into [table] values(%d, %d, %d)', [(1,2,3), (4,5,6)])
        '''
        try:
            if dbconn:
                conn = dbconn
            else:
                conn = pool.getconn()
                
            cur = conn.cursor()

            for i in xrange(0, len(vals), bulk_size):
                cur.executemany(sql, vals[i:i+bulk_size])
                
            if commit:
                conn.commit()
        except psycopg2.DatabaseError, e:
            LOGGER.error('pg bulk insert %d result error: %s' % (len(vals), e))
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                conn.close()

    @staticmethod
    def fetchone(sql):
        try:
            conn = pool.getconn()
            cur = conn.cursor()
            cur.execute(sql)
            res = cur.fetchone()
            if res:
                return res[0]
            else:
                return None
        except psycopg2.DatabaseError, e:
            LOGGER.error('pg bulk insert error: %s' % e)
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                conn.close()

    @staticmethod
    def fetchall(sql):
        try:
            conn = pool.getconn()
            cur = conn.cursor()
            cur.execute(sql)
            rows = cur.fetchall()
            return rows
        except psycopg2.DatabaseError, e:
            LOGGER.error('pg bulk insert error: %s' % e)
            if conn:
                conn.rollback()
            return None
        finally:
            if conn:
                conn.close()
        
SQL_INSERT_QUERY = 'INSERT INTO %s (%s) VALUES (%s)'

class LoadUtils:
    
    @staticmethod
    def fromCsvtodb(filepath,tablename,conn=None,cols=None,commit=False,bulkread_size=1000,split_char='\t'):
        '''
        commit = false 表示全部插入成功后在提交，否则表示每次插入都会提交
        cols = None 表示 columns从文件中读取，否则以给定list作为columns
        
        '''
        if not filepath or not os.path.exists(filepath):
            LOGGER.error("file path not exists:'%s'"%filepath)
            raise Exception("file path not exists:'%s'"%filepath)
        if conn is None:
            conn = pool.getconn()
        try:
            read_buffer=[]
            with open(filepath,'rb') as fr:
                for line in fr:
                    row = [i.strip() for i in line.strip().split(split_char)]
                    if not cols:
                        cols=row
                        continue
                    read_buffer.append(row)
                    if len(read_buffer) >= bulkread_size:
                        LoadUtils.todb(read_buffer,conn,tablename,cols,commit)
                        read_buffer=[]
            LoadUtils.todb(read_buffer,conn,tablename,cols,commit)
            if not commit:       
                conn.commit()
        except Exception,e:
            LOGGER.error("load file to db error,filepath: %s ,message: %s"%(filepath,e.message))
            import traceback
            ex = traceback.format_exc()
            LOGGER.error(ex)
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
                    
    @staticmethod
    def todb(table,conn,tablename,cols,commit):
        _cols = [_quote(col) for col in cols]
        colnames = ','.join(_cols)
        _placeholders = ','.join(['%s']*len(cols))
        sql = SQL_INSERT_QUERY % (_quote(tablename),colnames,_placeholders)
        try:
            DBUtils.bulkInsert(sql,table,conn, commit)
        except Exception,e:
            LOGGER.error("insert to db error,message:'%s'"%e.message)
            raise e
        
def _quote(s):
    quotechar='"'
    return quotechar + s + quotechar

if __name__ == '__main__':
    LoadUtils.fromCsvtodb("D:/tmp_file/testfile.csv", pool.getconn(), "test2", commit=False,bulkread_size=100, split_char=',')
    
