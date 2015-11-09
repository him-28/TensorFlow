#encoding=utf8

import psycopg2
import yaml
import sys

from datetime import datetime
from itertools import *

from etl.util import bearychat, init_log
from etl.conf.settings import Config, AuditConfig
#from utils import DBUtils

#Config = yaml.load(file("audit_config.yml"))
METRICS = AuditConfig.get('metrics')
LOG_FILE_PATH = AuditConfig["log_config_path"]
AUDIT_LOGGER = init_log.init(LOG_FILE_PATH, 'auditResInfoLogger')

class QualityAuditRobot(object):
    def __init__(self, date, hour): 
        self._date = date
        self._hour = hour
        self._title = u"phone_msite %s %s数据统计完成" % (date, hour)
        self._msgs = []
        self._metrics = []
        self._emitter = Message()
        self.db=Config["database"]["db_name"]
        self.db_user=Config["database"]["user"]
        self.db_password=Config["database"]["password"]
        self.db_host=Config["database"]["host"]
        self.db_port=Config["database"]["port"]

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
                msg = u'logic%d, %s 总计: %d' % (i, metric, res[0])
                self._msgs.append(msg)
                AUDIT_LOGGER.info("quality audit bot scan result: %s" % msg)
            except Exception, e:
                AUDIT_LOGGER.error("pg: %s, error: %s" % (sql, e))
                msg = u'logic%d, 异常' % i

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

        self._emitter.emit(self._title, self._msgs)

    def concat_sql(self, metric):
        '''
        @TODO: 此方法受表名, 度量值影响: metric, total
        '''
        sum_sqls = []

        if self._hour:
            table_name = METRICS[metric]['hour']
            if metric in ['reqs', 'code_serves']:
                sum_sqls.append('select sum(total) from "%s" where date_id=%s and time_id=%s' %(table_name, self._date, self._hour))
                sum_sqls.append('select sum(total) from "%s2" where date_id=%s and time_id=%s' %(table_name, self._date, self._hour))
            elif metric == 'impressions':
                sum_sqls.append('select sum(impressions_start_total) from "%s" where date_id=%s and time_id=%s' %(table_name, self._date, self._hour))
                sum_sqls.append('select sum(impressions_start_total) from "%s2" where date_id=%s and time_id=%s' %(table_name, self._date, self._hour))
            elif metric == 'click':
                sum_sqls.append('select sum(click) from "%s" where date_id=%s and time_id=%s' %(table_name, self._date, self._hour))
                sum_sqls.append('select sum(click) from "%s2" where date_id=%s and time_id=%s' %(table_name, self._date, self._hour))
        else:
            table_name = METRICS[metric]['day']
            if metric in ['reqs', 'code_serves']:
                sum_sqls.append('select sum(total) from "%s" where date_id=%s' %(table_name, self._date))
                sum_sqls.append('select sum(total) from "%s2" where date_id=%s' %(table_name, self._date))
            elif metric == 'impressions':
                sum_sqls.append('select sum(impressions_start_total) from "%s" where date_id=%s' %(table_name, self._date))
                sum_sqls.append('select sum(impressions_start_total) from "%s2" where date_id=%s' %(table_name, self._date))
            elif metric == 'click':
                sum_sqls.append('select sum(click) from "%s" where date_id=%s' %(table_name, self._date, self._hour))
                sum_sqls.append('select sum(click) from "%s2" where date_id=%s' %(table_name, self._date, self._hour))


        return sum_sqls

    def __str__(self):
        return (msg.join('\t') for msg in self._msgs) if self._msgs else ''


class Message:
    def __init__(self):
        pass

    def emit(self, title, msg):
        AUDIT_LOGGER.info("message begin emit: %s" % msg)
        msg_str = ''.join(m+'\r\n' for m in msg)
        AUDIT_LOGGER.info("message begin emit: %s" % msg_str)
        msg = '%s @QualityAuditRobot' % msg_str

        bearychat.send_message(title, msg)
        AUDIT_LOGGER.info("message emited")


if __name__ == '__main__':
    bot = QualityAuditBot('20151017', '22')
    bot.scan()

