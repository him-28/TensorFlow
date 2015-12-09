# encoding: utf-8
'''
Created on 2015年11月10日

@author: Administrator
'''
import sys
import os
reload(sys)
sys.setdefaultencoding('utf8')

import time
from datetime import datetime

from etl.util.bearychat import new_send_message
from etl.conf.settings import AuditConfig as Config
from etl.conf.settings import LOGGER
from etl.conf.settings import HEADER
from etl.conf.settings import AUDIT_HEADER
from etl.conf.settings import MONITOR_CONFIGS
from etl.util.playerutil import getplayerInfo

REPORT_CHANNEL = MONITOR_CONFIGS["bearychat_channel_hour"]

SUCCESS = "success"

fmt = ""
class ADMonitorAuditRobot(object):
    def __init__(self, total, problems, spent):
        """
        Record Total: total
        Problems: problems
        spent time: spent
        """
        self.total = total
        self.problems = problems
        self.spent = spent
        self.title = u"原始日志审计"
        self.channel = REPORT_CHANNEL
        self.normal = '#F8F8FF'
        self.error = '#FF0000'
        self.success = '#7FFFD4'

    def report(self):
        m_title = u'%s 数据审计完成' % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filename = self.problems["filename"]
        filesize = self.problems["filesize"]
        content = u"文件名{%s}, 大小%s\r\n" % (filename,filesize)
        content += u"总共%d纪录, 发现%d行错误,共耗时%d秒\r\n" % (self.total, self.problems.get('total', 0), self.spent)
        
        column_total = self.problems["column_total"]
        for k,v in column_total.items():
            content += u"字段: %s, 错误次数: %d\r\n" % (k, v)
            
        board_id_errors = self.problems["board_id_error"]
        if board_id_errors > 0:
            content += u"字段 board_id与slotid对应关系, 错误次数: %d\r\n" % (board_id_errors)
        sample = self.problems['sample']

        content += u"错误抽样样本%d条\r\n" % len(sample)

        for i in sample:
            content += i+"\r\n"

        if self.problems.get('total', 0) > 0:
            new_send_message(self.title , True, self.channel, m_title, content, self.error)
        else:
            new_send_message(self.title , True, self.channel, m_title, content, self.success)
            
class AdMonitor_audit:
    def __init__(self,fpath,filename):
        self.fpath = fpath
        self.filepath = os.path.join(fpath,filename)
        self.filename = filename
        self.error_rows = 0
        self.count_rows = 0
        self.sample_error_size = Config["sample_error_size"]
        self.header = HEADER
        self.problems = []
        self.columns_errors = {}
        self.spent = 0
        self.batch_write_size = Config["batch_write_size"]
        self.tmp_file_path = self.filepath + ".tmp"
        self.player_info = getplayerInfo()
        self.slotid_index = AUDIT_HEADER.index("slot_id")
        self.boardid_index = AUDIT_HEADER.index("board_id")
        self.s_ts_index = AUDIT_HEADER.index("server_timestamp")
        self.board_errors = 0
        
    def audit(self):
        start_time = time.time()
        first_row = Config["with_header"]
        if os.path.exists(self.tmp_file_path):
            os.remove(self.tmp_file_path)
        tag_index = AUDIT_HEADER.index("tag")
        file_split = Config["file_split"]
        write_buffer = []
        write_buffer.append(self.header)
        with open(self.filepath,'rb') as fr:
            for line in fr:
                new_line = line.strip("\r\n").strip("\n")
                if not new_line:
                    continue
                if first_row:
                    first_row = False
                    continue
                res = 0
                row = []
                self.count_rows = self.count_rows + 1
                try:
                    row=[i for i in new_line.split(file_split)]
                    res = self.validator(row,self.count_rows)
                    row[tag_index] = res
                    if len(row) != 32:
                        LOGGER.error("audit error,row length is %s ,not 32 行号：%s 行值：%s "%(len(row),str(self.count_rows),line))
                        continue
                except Exception,e:
                    LOGGER.error("audit error,行号：%s 行值：%s ,error message:%s"%(str(self.count_rows),line,e.message))
                    continue
                #row[tag_index] = res
                write_buffer.append(row)
                if len(write_buffer) >= self.batch_write_size:
                    self.write_file(write_buffer)
                    write_buffer = []
                    
        self.write_file(write_buffer)
        os.remove(self.filepath)
        os.rename(self.tmp_file_path,self.filepath)
        write_buffer = []
        end_time = time.time()
        self.spent = end_time - start_time
        
    def write_file(self,write_buffer):
        with open(self.tmp_file_path, 'a') as f:
            for row in write_buffer:
                s = MONITOR_CONFIGS["output_column_sep"].join(str(e) for e in row)
                f.write(s+Config["line_n"])
        
    def report(self):
        sample = []
        max_s = self.sample_error_size
        if self.error_rows < self.sample_error_size:
            max_s = self.error_rows
        for i in range(0,max_s):
            sample.append(self.problems[i])
            
        filesize = self.getfilesize()
        total_problems = {'column_total': self.columns_errors,'board_id_error':self.board_errors,'total': self.error_rows, 'sample': sample,'filename':self.filename,'filesize':filesize}
        robot = ADMonitorAuditRobot(self.count_rows, total_problems, self.spent)
        robot.report()
            
    def validator(self,row,index):
        flag = 0
        for field in Config["validate_fields"]:
            result = self.validate_field(index,row,field)
            if result > flag:
                flag = result
        result = self.validate_slot_id(row,index)
        if result > flag:
            flag = result
        if flag > 99:
            self.error_rows = self.error_rows + 1
        return  flag

    def validate_slot_id(self,row,index):
        if not row[self.slotid_index]:
            return 0
        try:
            slot_id = int(row[self.slotid_index])
            board_id = int(row[self.boardid_index])
            s_timestamp= float(row[self.s_ts_index])
        except:
            return 101

        for v in self.player_info.values():
            start = v.get('starttime')
            end = v.get('endtime')
            if s_timestamp > start and s_timestamp < end:
                if v['playerinfo'].has_key(board_id):
                    if v['playerinfo'][board_id].has_key(slot_id):
                        return 0
        problem = "行%s,列：%s 值：%s 错误信息：%s"%(str(index),"board_id",str(board_id),"not matched slotid:%s" % slot_id)
        self.problems.append(problem)
        self.board_errors = self.board_errors + 1
        return 102
    def validate_field(self,index,row,field_name):
        _index = self.header.index(field_name)
#         print row
        value = row[_index]
        scenes = Config["field_rule"][field_name]
        result = self._validate_value(row,value, scenes)
        if result == SUCCESS:
            return 0
#         self.error_rows = self.error_rows + 1
        problem = "行%s,列：%s 值：%s 错误信息：%s"%(str(index),field_name,value,result)
        self.problems.append(problem)
        c = self.columns_errors.get(field_name,0)
        c = c + 1
        self.columns_errors.update({field_name:c})
        return 101
        
    def _validate_value(self,row,value,scenes):
        for k,v in scenes.items():
            condition = v.get("condition")
            result = self.filter_by_condition(row, condition)
            if not result:   #如果不在条件内，则不考虑规则
                continue
            required = v.get("required")
            if required and (not value or not value.strip()):
                msg = "value is null"
                return msg
            
            if not value or not value.strip():
                return SUCCESS
            
            if v.has_key("vtype"):
                vtype = v.get("vtype")
                if vtype == Config["int"]:
                    try:
                        int(value)
                    except:
                        msg = "value type is error"
                        return msg
                elif vtype == Config["float"]:
                    try:
                        float(value)
                    except:
                        msg = "value type is error"
                        return msg
                    
            reg = v.get("reg")
            reg_result = self.match_reg(value, reg)
            if not reg_result:
                msg = "value not correct"
                return msg
            
            ran = v.get("range")
            if not ran:
                continue
            if value not in ran:
                msg = "value not in range"
                return msg
            
        return SUCCESS
            
    def filter_by_condition(self,row,condition):
        if not condition:
            return True
        for k,v in condition.items():
            k_index = self.header.index(k)
            if row[k_index] in v:
                continue
            else:
                return False
        return True
    
    def match_reg(self,value,reg):
        #TODO 根据正则匹配
        return True
    def getfilesize(self):
        psize = os.path.getsize(self.filepath)
        filesize = '%0.3f' % (psize/1024.0/1024.0)
        return str(filesize)+"MB"
    
def ad_audit(filepath,filename):
    try:
        ad = AdMonitor_audit(filepath,filename)
        LOGGER.info("audit log ...")
        ad.audit()
        ad.report()
    except Exception,e:
        LOGGER.error("audit log file error,filepath:%s%s error message: %s"%(filepath,filename,e.message))
        import traceback
        ex=traceback.format_exc()
        LOGGER.error(ex)
        sys.exit(-1)
        
if __name__ == "__main__":
    import time
    l1 = time.time()
    filepath = r"C:\Users\Administrator\Desktop\ad_test_xx.csv"
    ad = AdMonitor_audit(filepath)
    ad.audit()
    print len(ad.problems)
    for line in ad.problems:
        print line.encode("utf-8")
    ad.report()
