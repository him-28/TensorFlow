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
import init_log
import yaml
from pymongo import MongoClient
import uuid

CONFIG = yaml.load(file("audit_config.yml"))
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
        if type(line) == unicode:
            return line
        elif type(line) == str:
            return line.decode('utf8','ignore')
        else:
            _ = str(line)
            return _.replace("ObjectId(", "").rstrip(")")

    def logKeyAggregation(self, request_row):
        key_aggre_dict = {}
        key_aggre_dict['sid'] = str(uuid.uuid4())
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
        print key_aggre_dict

    def logRowAudit(self, row_dict, key_list, value_type, table_name):
        '''
        '''

    def supplyLogAudit(self):
        '''
        '''
        for board_id in self.board_list:
            table_name = "%s_%s_%s_%s" % (self.supply_prefix_name, str(board_id), self.run_year_day,
                    self.run_hour)
            query_filter = {'p.v.title':0,'p.v.keyword':0,'p.v.rname':0,'p.c.ua':0, 'p.v.hname':0, 'p.v.sub_type':0,'p.v.name':0}
            rows_data = self.getDBTableNameAllData(table_name, query_filter)
            #row_len = rows_data.length
            #if row_len == 0:
            #    ADUIT_LOGGER.error("%s data row len is zero!" % table_name)
            #    continue
            #ADUIT_LOGGER.info("%s table row len is %d" % (table_name, row_len))
            supply_save_file = "%s/%s.%s.supply.csv" % (self.supply_save_path,
                    self.run_year_day[:8], self.run_hour)
            with open(supply_save_file, "a") as fw:
                for request_row in rows_data:
                    log_dict = self.logKeyAggregation(request_row)
                    self.logRowAudit(log_dict, self.supply_key_list, self.supply_value_type,
                            table_name)
            
if __name__ == '__main__':
    if len(sys.argv) != 3:
        ADUIT_LOGGER.error("argv len not 2(yearday hour), argv is %s" % '\t'.join(sys.argv))
        sys.exit(-1)
    (dir, year_day, hour) = sys.argv
    adlog_format_audit = AdlogFormatAudit(year_day, hour)
    adlog_format_audit.supplyLogAudit()
    sys.exit(0)
## vim: set ts=2 sw=2: #

