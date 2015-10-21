#encoding=utf-8

import os
import psycopg2

from psycopg2.pool import ThreadedConnectionPool

from etl.conf.settings import LOGGER
from etl.conf.settings import AuditConfig as CONFIG


class DBConfig:
    
    @staticmethod
    def defaultConfig():
        return {
            'database':  'test2',
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
        return config.update({
            'database': CONFIG['database']['db_name'],
            'user': CONFIG['database']['user'],
            'password': CONFIG['database']['password'],
            'host': CONFIG['database']['host'],
            'port': CONFIG['database']['port'],
            'minconn': CONFIG['database']['minconn'],
            'maxconn': CONFIG['database']['maxconn']
            })


config = DBConfig.defaultConfig()


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
        #database=self.db, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port
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
        return self._pool.getconn()


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

    @staticmethod
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
pool = SafePoolManager(config['minconn'], config['maxconn'],test=12232 )