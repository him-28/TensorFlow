# encoding=utf8
'''
Created on 2015年10月20日

@author: Administrator
'''
from etl.conf.settings import AuditConfig
from etl.util import init_log, bearychat

LOG_FILE_PATH = AuditConfig["log_config_path"]
LOG = init_log.init(LOG_FILE_PATH, 'auditFullInfoLogger')

# 对比表名称
TABLE_ADDON = '2'

def get_compare_table_name(table_name):
    '''获取对比表名'''
    return table_name + TABLE_ADDON

def get_title_fmt(title):
    '''标题格式化字符串'''
    fmt = "|"
    for tit in title:
        lens = len(tit)
        if lens < 10:
            lens = 10
        fmt = fmt + "%-" + str(lens) + "s| "
    return fmt

def trans_arr_by_line(arr):
    '''把List转换成按行排列字符串'''
    msg = '\n'
    for item in arr:
        msg = msg + str(item) + "\n"
    return msg

def trans_audit_result(arr, title):
    '''处理审计结果'''
    msg = ''
    for item in arr:
        msg = msg + "\n" + str(item)
        LOG.warn(item)
    #bearychat.new_send_message(None, at_title=title, at_text=msg)
    return msg

def audit_count(count1, count2, c_type):
    '''比较行数，如果有任何一个count为0，返回False，否则返回True'''
    msg = 'logic0,%s 总计%s条数据\nlogic1,%s 总计%s条数据\n'\
        % (c_type, count1, c_type, count2)
    if count1 == 0L or count2 == 0L:
        return False, msg + '差异数据条数:%s\n' % (count1 + count2)
    return True, msg

class QualityFullAuditRobot(object):
    ''' QualityFullAuditRobot '''
    def __init__(self):
        self._metrics = AuditConfig.get("metrics")
        self.db_util = DBUtil()
        self.diff_list = []
        self._date = None
        self._hour = None
        self._title = None

    def audit(self, audit_date, audit_hour=None):
        '''审计，审计天数据则把audit_hour设为None'''
        audit_date = str(audit_date)
        if audit_hour is None:
            self._title = "%s 数据集审计结果：" % audit_date
            # self.diff_list.append("%s 天数据审核结果：" % audit_date)
            self.audit_day(audit_date)
        else:
            audit_hour = str(audit_hour)
            self._title = "%s %s 数据集审计结果：" % (audit_date, audit_hour)
            self.audit_hour(audit_date, audit_hour)

        msg = trans_audit_result(self.diff_list, self._title)
        return msg

    def audit_hour(self, audit_date, audit_hour):
        '''审计小时数据'''
        self._date = audit_date
        self._hour = audit_hour
        LOG.info("audit hour:" + audit_date + " " + audit_hour)
        for (c_type, rules) in self._metrics.items():
            hour_sql1, hour_sql2, from_table, to_table = \
                self.__get_hour_sql(rules)
            LOG.info("compare:" + c_type)
            count1 = self.__count_datas(from_table)
            count2 = self.__count_datas(to_table)
            count_audit_result, count_audit_msg = audit_count(count1, count2, c_type)
            if count_audit_result:
                sql_audit_result, sql_audit_msg = self.__audit_sql\
                    (hour_sql1, hour_sql2, from_table, to_table)
                if sql_audit_result:  # 没有差异数据
                    pass
                else:
                    self.diff_list.append(count_audit_msg + sql_audit_msg)
            else:
                self.diff_list.append(count_audit_msg)

    def audit_day(self, audit_date):
        '''审计天数据'''
        self._date = audit_date
        LOG.info("audit day:" + audit_date)

        for (c_type, rules) in self._metrics.items():
            day_sql1, day_sql2, from_table, to_table = self.__get_day_sql(rules)
            LOG.info("compare:" + c_type)
            count1 = self.__count_datas(from_table)
            count2 = self.__count_datas(to_table)
            count_audit_result, count_audit_msg = audit_count(count1, count2, c_type)
            if count_audit_result:
                sql_audit_result, sql_audit_msg = self.__audit_sql\
                    (day_sql1, day_sql2, from_table, to_table)
                if sql_audit_result:  # 没有差异数据
                    pass
                else:
                    self.diff_list.append(count_audit_msg + sql_audit_msg)
            else:
                self.diff_list.append(count_audit_msg)

    def __count_datas(self, table_name):
        '''查询数据条数'''
        if self._hour is None:
            sql = 'SELECT COUNT(0) FROM "%s" WHERE date_id=%s' % (table_name, self._date)
        else:
            sql = 'SELECT COUNT(0) FROM "%s" WHERE date_id=%s AND time_id=%s'\
                % (table_name, self._date, self._hour)
        result = self.db_util.query(sql)[0][0][0]
        return result

    def __audit_sql(self, sql1, sql2, from_table, to_table):
        '''比对结果，返回是否存在不一到数据、不一致具体信息'''
        result1, title1 = self.db_util.query(sql1)
        len1 = len(result1)
        details1 = []
        details2 = []
        if len1 > 0:
            details1 = self.__handle_diff(result1, title1, from_table, to_table)
            LOG.error(trans_arr_by_line(details1))

        result2, title2 = self.db_util.query(sql2)
        len2 = len(result2)
        if len2 > 0:
            details2 = self.__handle_diff(result2, title2, to_table, from_table)
            LOG.error(trans_arr_by_line(details2))
        result = len1 == 0 and len2 == 0
        msg = '差异数据条数:%s\n' % (len1 + len2)
        return result, msg

    def __handle_diff(self, result, header, from_table, to_table):
        '''处理不同'''
        details = []
        date_str = str(self._date)
        if self._hour is None:
            date_str = date_str + " " + str(self._hour)
        msg = "%s数据 %s和 %s相比，有以下不同：\n" % (date_str, from_table, to_table)
        fmt = get_title_fmt(header)
        msg = msg + (fmt % tuple(header)) + "\n"
        for res in result:
            msg = msg + (fmt % tuple(res)) + "\n"
        details.append(msg)
        return details

    def __get_day_sql(self, rules):
        '''拼接天比对SQL'''
        audit_date = self._date
        table_name = rules.get("day")
        compare_table_name = get_compare_table_name(table_name)

        compare_field = rules.get("compare_field")
        sql = 'SELECT * FROM "%s" T1 WHERE T1.date_id=\'%s\' \
AND NOT EXISTS(SELECT 1 FROM "%s" T2 WHERE T2.date_id=T1.date_id '

        for field in compare_field:
            sql = sql + " AND " + " T1." + field + "=" + "T2." + field
        sql = sql + ');'

        sql1 = sql % (table_name, audit_date, compare_table_name)
        sql2 = sql % (compare_table_name, audit_date, table_name)
        return sql1, sql2, table_name, compare_table_name


    def __get_hour_sql(self, rules):
        '''拼接小时比对SQL'''
        audit_date = self._date
        audit_hour = self._hour
        table_name = rules.get("hour")
        compare_table_name = get_compare_table_name(table_name)

        compare_field = rules.get("compare_field")
        sql = 'SELECT * FROM "%s" T1 WHERE T1.date_id=\'%s\' AND T1.time_id=\'%s\' AND NOT EXISTS\
(SELECT 1 FROM "%s" T2 WHERE T1.date_id=T2.date_id AND T1.time_id=T2.time_id'

        for field in compare_field:
            sql = sql + " AND " + " T1." + field + "=" + "T2." + field
        sql = sql + ');'

        sql1 = sql % (table_name, audit_date, audit_hour, compare_table_name)
        sql2 = sql % (compare_table_name, audit_date, audit_hour, table_name)
        LOG.info("split day sql[1]:" + sql1)
        LOG.info("split day sql[2]:" + sql2)
        return sql1, sql2, table_name, compare_table_name

from etl.conf.settings import Config
import psycopg2
from psycopg2 import ProgrammingError

class DBUtil(object):
    '''temp DBUtil'''
    def __init__(self):
        self.database = Config["database"]["db_name"]
        self.db_user = Config["database"]["user"]
        self.db_password = Config["database"]["password"]
        self.db_host = Config["database"]["host"]
        self.db_port = Config["database"]["port"]

        # connect info
        LOG.info("connect --> db:" + self.database + ",user:" + \
                self.db_user + ",password:***,host:" + str(self.db_host)\
                + ",port:" + str(self.db_port) + "...")

    def connect(self):
        '''返回新的Db连接'''
        conn = psycopg2.connect(database=self.database, user=self.db_user, \
                              password=self.db_password, host=self.db_host, port=self.db_port)
        return conn

    def query(self, sql):
        '''执行一个sql查询'''
        LOG.info("query sql:" + sql)
        conn = self.connect()
        cur = conn.cursor()
        results = []
        title = []
        try:
            cur.execute(sql)
            results = cur.fetchall()
            des = cur.description
            des_len = len(des)
            for i in range(des_len):
                title.append(des[i][0])
        except ProgrammingError as p_e:
            LOG.error("执行SQL错误：" + p_e.message + "check your sql:\n" + sql)
            raise
        cur.close()
        conn.close()
        return results, title

if __name__ == '__main__':
    import sys
    THE_DAY = sys.argv[1]
    THE_HOUR = None
    if len(sys.argv) > 1:
        THE_HOUR = sys.argv[2]
    QualityFullAuditRobot().audit(THE_DAY, THE_HOUR)
