#encoding=utf8

import psycopg2
import yaml

from datetime import datetime
from itertools import *

from util import bearychat, init_log
#from utils import DBUtils

CONFIG = yaml.load(file("audit_config.yml"))
METRICS = CONFIG.get('metrics')
LOG_FILE_PATH = CONFIG["log_config_path"]
AUDIT_LOGGER = init_log.init(LOG_FILE_PATH, 'auditResInfoLogger')

class QualityAuditBot(object):
    def __init__(self, date, hour): 
        self._date = date
        self._hour = hour
        self._msgs = []
        self._metrics = []
        self._emitter = Message()
        self.db=CONFIG["database"]["db_name"]
        self.db_user=CONFIG["database"]["user"]
        self.db_password=CONFIG["database"]["password"]
        self.db_host=CONFIG["database"]["host"]
        self.db_port=CONFIG["database"]["port"]

    def statistic(self, metric, sqls=[]):
        #
        #@TODO: DBUtils function
        #
        conn=psycopg2.connect(database=self.db, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port)
        cur = conn.cursor()
        for i, sql in enumerate(sqls):
            try:
                cur.execute(sql)
                res = cur.fetchone()
                conn.commit()
                #res = DBUtils.excute_by_sql(sql)
                msg = u'logic%d,%s 总计: %d' % (i, metric, res[0])
                self._msgs.append(msg)
                AUDIT_LOGGER.info("quality audit bot scan result: %s" % msg)
            except Exception, e:
                AUDIT_LOGGER.error("pg: %s, error: %s" % (sql, e))

        if cur:
            cur.close()
        if conn:
            conn.close()

    def scan(self):
        AUDIT_LOGGER.info("quality audit bot begin scan: %s,%s" % (self._date, self._hour))
        for k, v in METRICS.iteritems():
            self._metrics.append(k)
        AUDIT_LOGGER.info("quality audit bot scan metrics: %s" % self._metrics)
        for metric in self._metrics:
            sum_sqls = self.concat_sql(metric)
            AUDIT_LOGGER.info("quality audit bot scan sql: %s" % sum_sqls)
            self.statistic(metric, sum_sqls)

        self._emitter.emit(self._msgs)

    def concat_sql(self, metric):
        '''
        @TODO: 此方法受表名, 度量值影响: metric, total
        '''
        sum_sqls = []

        if self._hour:
            table_name = METRICS[metric]['hour']
            sum_sqls.append('select sum(total) from "%s" where date_id=%s and time_id=%s' %(table_name, self._date, self._hour))
            sum_sqls.append('select sum(total) from "%s2" where date_id=%s and time_id=%s' %(table_name, self._date, self._hour))
        else:
            table_name = METRICS[metric]['day']
            sum_sqls.append('select sum(total) from "%s" where date_id=%s' %(table_name, self._date, self._hour))
            sum_sqls.append('select sum(total) from "%s2" where date_id=%s' %(table_name, self._date, self._hour))

        return sum_sqls

    def __str__(self):
        return (msg.join('\t') for msg in self._msgs) if self._msgs else ''


class Message:
    def __init__(self):
        pass

    def emit(self, msg):
        AUDIT_LOGGER.info("message begin emit: %s" % msg)
        msg_str = ''.join(m+'\r\n' for m in msg)
        AUDIT_LOGGER.info("message begin emit: %s" % msg_str)
        msg = '%s @QualityAuditBot' % msg_str
        bearychat.send_message(msg)
        AUDIT_LOGGER.info("message emited")


if __name__ == '__main__':
    from pdb import set_trace as st
    bot = QualityAuditBot('20151016', '01')
    bot.scan()
