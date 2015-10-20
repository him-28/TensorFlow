#encoding=utf-8

import os
import psycopg2

from psycopg2.pool import ThreadedConnectionPool

from etl.conf.settings import LOGGER
from etl.conf.settings import AUDIT_CONFIG as CONFIG


class DBConfig:

    def defaultConfig():
        return {
            'database':  'test',
            'user': 'test',
            'password': 'test',
            'host': '127.0.0.1',
            'port': '27091',
            'mincached': 5,
            'maxcached': 50
        }

    def Config():
        config = DBConfig.defaultConfig()
        return config.update({
            'database': CONFIG['database']['db_name'],
            'user': CONFIG['database']['user'],
            'password': CONFIG['database']['password'],
            'host': CONFIG['database']['host'],
            'port': CONFIG['database']['port'],
            'minconn': CONFIG['database']['minconn'],
            'maxconn': CONFIG['database']['maxconn']
        )}


config = DBConfig.Config()
pool = SafePoolManager(config.minconn, config.maxconn, config.host, config.port)

class SafePoolManager:
    """
    pool = SafePoolManager(1, 10, "host='127.0.0.1' port=12099")
    """

    def __init__(self, *args, **kwargs):
        self.last_seen_process_id = os.getpid()
        self.args = args
        self.kwargs = kwargs
        self._init()

    def _init(self):
        self._pool = ThreadedConnectionPool(*args, **kwargs)

    def getconn(self):
        current_pid = os.getpid()
        if not (current_pid == self.last_seen_process_id):
            self._init()
            LOGGER.info("New id is %s, old id was %s" % (current_pid, self.last_seen_process_id))
            self.last_seen_process_id = current_pid
        return self._pool.putconn(conn)


class DBUtils:
    """
    insert: DBUtils.insert('insert into [table] values(1,2,3)')
    bulkInsert: DBUtils.bulkInsert('insert into [table] values(%d, %d, %d)', [(1,2,3), (4,5,6)])
    """
    
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

    def bulkInsert(sql, vals=[]):
        try:
            conn = pool.getconn()
            cur = conn.cursor()

            for i in xrange(0, len(vals), 1000):
                cur.executemany(sql, vals[i:i+1000])
                conn.commit()

        except psycopg2.DatabaseError, e:
            LOGGER.error('pg bulk insert %d result error: %s' % (len(vals), e))
            if conn:
                conn.rollback()
            return None

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
