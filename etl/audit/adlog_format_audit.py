#!/usr/bin/python
# -*- coding: utf-8 -*- #
############################################################################
## 
## Copyright (c) 2013 hunantv.com, Inc. All Rights Reserved
## $Id: adlog_format_audit.py,v 0.0 2015年10月09日 星期一 15时44分56秒  dongjie Exp $ 
## 
############################################################################
#
###
# # @file   adlog_format_audit.py 
# # @author dongjie<dongjie@e.hunantv.com>
# # @date   2015年10月09日 星期一 15时44分56秒  
# # @brief mongodb 日志导出与格式校验
# #  
# ##
import sys
from etl.util import init_log
import yaml
from pymongo import MongoClient
import uuid
import types
import psycopg2
import os
from etl.util import bearychat
from etl.conf.settings import AUDIT_CONFIG as CONFIG

LOG_FILE_PATH = CONFIG["log_config_path"]
ADUIT_LOGGER = init_log.init(LOG_FILE_PATH, 'aduitInfoLogger')

class AdlogFormatAudit:
    '''
    '''

    def __init__(self, year_day, hour):
        '''
        '''
        self.mongodb_host_list = CONFIG["mongodb"]["host_list"]
        self.mongodb_port = CONFIG["mongodb"]["port"]
        self.supply_prefix_name = CONFIG["supply_prefix_name"]
        self.demand_prefix_name = CONFIG["demand_prefix_name"]
        self.board_list = CONFIG["board_list"]
        self.supply_save_path = CONFIG["supply_save_path"]
        self.supply_key_list = CONFIG["supply_key_list"]
        self.supply_value_type = CONFIG["supply_value_type"]
        self.supply_value_isnull = CONFIG["supply_value_isnull"]
        self.demand_save_path = CONFIG["demand_save_path"]
        self.demand_key_list = CONFIG["demand_key_list"]
        self.demand_value_type = CONFIG["demand_value_type"]
        self.demand_value_isnull = CONFIG["demand_value_isnull"]
        self.boardid_dict = CONFIG["boardid_dict"]
        self.db=CONFIG["database"]["db_name"]
        self.db_user=CONFIG["database"]["user"]
        self.db_password=CONFIG["database"]["password"]
        self.db_host=CONFIG["database"]["host"]
        self.db_port=CONFIG["database"]["port"]
        self.run_year_day = year_day
        self.run_hour = hour
        pass

    def getDBTableNameAllData(self, table_name, query_filter):
        '''
        '''
        try:
            client = MongoClient(self.mongodb_host_list, self.mongodb_port, slaveOK=False)
        except Exception, e:
            ADUIT_LOGGER.warning(e.message)
            client = MongoClient(self.mongodb_host_list, self.mongodb_port)
        try:
            db_client = client.emap
            table_client = db_client[str(table_name)]
            return table_client.find({}, query_filter)
        except Exception, e:
            ADUIT_LOGGER.error("get table %s data error,%s" % (table_name, e.message))
            return []

    def getstr(self, line):
        if type(line) == unicode or type(line) == list:
            return line
        elif type(line) == str:
            return line.decode('utf8','ignore')
        else:
            _ = str(line)
            return _.replace("ObjectId(", "").rstrip(")")

    def logKeyAggregation(self, request_row):
        key_aggre_dict = {}
        key_aggre_dict['sid'] = str(uuid.uuid4())
        key_aggre_dict['p_v_rname'] = str('')
        for k_1, v_1 in request_row.items():
            if type(v_1) == dict:
                for k_2, v_2 in v_1.items():
                    if type(v_2) == dict:
                        for k_3, v_3 in v_2.items():
                            key = "{0}_{1}_{2}".format(k_1,k_2,k_3)
                            key_aggre_dict[key] = self.getstr(v_3)
                    else:
                        key = "{0}_{1}".format(k_1,k_2)
                        key_aggre_dict[key] = self.getstr(v_2)
            else:
                key_aggre_dict[k_1] = self.getstr(v_1)
        return key_aggre_dict

    def valueTypeConform(self, value, value_type):
        '''
        return -1 value type != value_type
        '''
        if value_type == 'int':
            if type(value) is int:
                return 0
            if (type(value) is str or type(value) is unicode) and value.isdigit() == True:
                return 0
        elif value_type == 'str':
            if type(value) is str or type(value) == unicode:
                return 0
        elif value_type == 'list_int' and type(value) == list:
            for _v in value:
                if type(_v) != types.IntType:
                    return -1
            return 0
        return -1

    def getConn(self):           
        '''
        '''
        conn=psycopg2.connect(database=self.db, user=self.db_user, password=self.db_password, host=self.db_host, port=self.db_port)
        return conn
     
    def excute_by_sql(self, str_sql):
        '''
        '''
        try:
            conn = self.getConn()
            cur = conn.cursor()
            if not str_sql:
                return 
            result = cur.execute(str_sql)
            conn.commit()
        except Exception,e:
            ADUIT_LOGGER.error("pg %s error:%s" % (str_sql, e))
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
 
    def logRowAudit(self, row_dict, key_list, value_type, value_isnull, table_name):
        '''
        '''
        key_list_len = len(key_list)
        if len(value_type) != key_list_len:
            ADUIT_LOGGER.error("value_type len not key_list len, value_type_len:%d  key_list_len:%d"
                    % (len(value_type), key_list_len))
            return -1
        if len(value_isnull) != key_list_len:
            ADUIT_LOGGER.error("value_isnull len not key_list len, value_isnull:%d  key_list_len:%d"
                    % (len(value_type), key_list_len))
            return -1
        _id = row_dict.get('_id', '-1')
        error_detail_sql = "INSERT INTO \"Data_Audit_Details\" VALUES (\'%s\', \'%s\', \'%s\',\' %s\', \'%s\', \'%s\');"
        for i in range(key_list_len):
            key_name = key_list[i]
            value_type_str = value_type[i]
            value_isnull_bool = value_isnull[i]
            if row_dict.has_key(key_name) == False:
                if key_name in ['slotid', 'cardid', 'creativeid']:
                    row_dict[key_name] = []
                else:
                    self.excute_by_sql(error_detail_sql % (self.run_year_day + self.run_hour, table_name, key_name, '', 'log not key', _id))
                    #insert error pg
                    ADUIT_LOGGER.error("row_dict not key %s, table_name: %s _id:%s" % (key_name,
                        table_name, _id))
                    return 1
            _value = row_dict[key_name]
            if (value_isnull_bool == 0 and (_value == '' or _value == None)):
                self.excute_by_sql(error_detail_sql % (self.run_year_day + self.run_hour, table_name, key_name, '', 'key is null', _id))
                #insert error pg
                ADUIT_LOGGER.error("key %s is null, table_name: %s _id:%s" % (key_name,
                    table_name, _id))
                return 1
            if _value != '' and _value != None and self.valueTypeConform(_value, value_type_str) == -1:
                error_detail = "%s value %s type not %s" % (key_name, str(_value), value_type_str)
                self.excute_by_sql(error_detail_sql % (self.run_year_day + self.run_hour, table_name, key_name, str(_value), error_detail, _id))
                #insert error pg
                ADUIT_LOGGER.error("%s value %s type not %s, table_name: %s _id:%s" % (key_name,
                    str(_value), value_type_str, table_name, _id))
                return 2
        return 0

    def slotidSpread(self, log_dict, key_list, table_name, fw):
        '''
        '''
        format_str = ""
        boardid = str(log_dict.get('boardid', 0))
        boardid_list = self.boardid_dict.get(boardid, [])
        slotid_list = log_dict.get('slotid', [])
        cardid_list = log_dict.get('cardid', [])
        createiveid_list = log_dict.get('creativeid', [])
        for _k in key_list:
            if format_str != '':
                format_str += '\t'
            if _k in ['slotid', 'cardid', 'creativeid']:
                format_str += "%s"
            elif _k == 'p_v_rname':
                format_str += log_dict['p_v_rname'].encode('utf-8')
            else:
                format_str += str(log_dict.get(_k, "-1").encode('utf-8'))
        s_len = len(slotid_list)
        if s_len != len(cardid_list) or s_len != len(createiveid_list):
            return ''
        tmp_boardid_list = boardid_list[:]
        for _id in range(s_len):
            slot_id = str(slotid_list[_id])
            if slot_id in boardid_list:
                tmp_boardid_list.remove(slot_id)
            else:
                #insert pg
                pass
            fw.write(format_str % (slot_id, str(cardid_list[_id]),
                    str(createiveid_list[_id])) + '\n')
        for s_id in tmp_boardid_list:
            fw.write(format_str % (s_id, "-1", "-1") + '\n')

    def get_probability(self, deno, molecular):
        '''
        '''
        if deno == 0:
            return 0.0
        else:
            return float(molecular) / float(deno)

    def supplyLogAudit(self):
        '''
        '''
        save_path = "%s/%s/" % (self.supply_save_path, self.run_year_day[:6])
        supply_save_file = "%s.%s.product.supply.csv" % (self.run_year_day[:8], self.run_hour)
        if os.path.exists(save_path) == False:
            os.makedirs(save_path)
        with open(save_path + supply_save_file, "w") as fw:
            check_total = 0
            error_total = 0
            key_error_total = 0
            value_type_error_total = 0
            for board_id in self.board_list:
                table_name = "%s_%s_%s_%s" % (self.supply_prefix_name, str(board_id), self.run_year_day,
                        self.run_hour)
                query_filter = {'p.v.title':0,'p.v.keyword':0,'p.c.ua':0, 'p.v.hname':0,
                        'p.v.sub_type':0,'p.v.name':0,'p.v.rname':0}
                ADUIT_LOGGER.info("table %s start load" % (table_name))
                rows_data = self.getDBTableNameAllData(table_name, query_filter)
                for request_row in rows_data:
                    check_total += 1
                    log_dict = self.logKeyAggregation(request_row)
                    rt = self.logRowAudit(log_dict, self.supply_key_list, self.supply_value_type,
                            self.supply_value_isnull, table_name)
                    if rt != 0:
                        error_total += 1
                        continue
                    if rt == 1:
                        key_error_total += 1
                    elif rt == 2:
                        value_type_error_total += 1
                    self.slotidSpread(log_dict, self.supply_key_list, table_name, fw)
        self.excute_by_sql("INSERT INTO \"Data_Audit_Statistics\" VALUES (\'%s\', \'%s\', %d, %f)" % (supply_save_file, self.run_year_day + self.run_hour, check_total, self.get_probability(check_total, error_total)))  
        ADUIT_LOGGER.info("table_name:%s|check_total:%d|rf:%f" % (supply_save_file, check_total, self.get_probability(check_total, error_total)))
        message_str = "phone_m adlog file %s total:%d\n error_totale:%d\n rf:%f\n key_error_total:%d\n value_type_error_total:%d"
        bearychat.send_message(message_str % (supply_save_file, check_total, error_total, self.get_probability(check_total, error_total), key_error_total, value_type_error_total))
 
    def demandLogAudit(self):
        '''
        '''
        save_path = "%s/%s/" % (self.demand_save_path, self.run_year_day[:6])
        demand_save_file = "%s.%s.product.demand.csv" % (self.run_year_day[:8], self.run_hour)
        if os.path.exists(save_path) == False:
            os.makedirs(save_path)
        with open(save_path + demand_save_file, "w") as fw:
            check_total = 0
            error_total = 0
            key_error_total = 0
            value_type_error_total = 0
            for board_id in self.board_list:
                table_name = "%s_%s_%s_%s" % (self.demand_prefix_name, str(board_id), self.run_year_day,
                        self.run_hour)
                query_filter = {'p.v.title':0,'p.v.keyword':0,'p.c.ua':0, 'p.v.hname':0,
                        'p.v.sub_type':0,'p.v.name':0,'p.v.rname':0}
                ADUIT_LOGGER.info("table %s start load" % (table_name))
                rows_data = self.getDBTableNameAllData(table_name, query_filter)
                for request_row in rows_data:
                    check_total += 1
                    log_dict = self.logKeyAggregation(request_row)
                    rt = self.logRowAudit(log_dict, self.demand_key_list, self.demand_value_type,
                            self.demand_value_isnull, table_name)
                    if rt != 0:
                        error_total += 1
                        continue
                    if rt == 1:
                        key_error_total += 1
                    elif rt == 2:
                        value_type_error_total += 1
                    format_str = ''
                    for _k in self.demand_key_list:
                        if format_str != '':
                            format_str += '\t'
                        format_str += str(log_dict.get(_k, "-1"))
                    fw.write(format_str + '\n')
        self.excute_by_sql("INSERT INTO \"Data_Audit_Statistics\" VALUES (\'%s\', \'%s\', %d, %f)" % (demand_save_file, self.run_year_day + self.run_hour, check_total, self.get_probability(check_total, error_total)))  
        ADUIT_LOGGER.info("table_name:%s|check_total:%s|rf:%f" % (demand_save_file, check_total, self.get_probability(check_total, error_total)))
        message_str = "phone_m adlog file %s total:%d\n error_totale:%d\n rf:%f\n key_error_total:%d\n value_type_error_total:%d"
        bearychat.send_message(message_str % (demand_save_file, check_total, error_total, self.get_probability(check_total, error_total), key_error_total, value_type_error_total))

def run_aduit_adlog(year_day, hour):
    adlog_format_audit = AdlogFormatAudit(year_day, hour)
    adlog_format_audit.supplyLogAudit()
    adlog_format_audit.demandLogAudit()
                    
if __name__ == '__main__':
    if len(sys.argv) != 3:
        ADUIT_LOGGER.error("argv len not 2(yearday hour), argv is %s" % '\t'.join(sys.argv))
        sys.exit(-1)
    (dir, year_day, hour) = sys.argv
    adlog_format_audit = AdlogFormatAudit(year_day, hour)
    adlog_format_audit.supplyLogAudit()
    adlog_format_audit.demandLogAudit()
    sys.exit(0)
## vim: set ts=2 sw=2: #

