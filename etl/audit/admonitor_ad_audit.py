# encoding: utf-8
'''
Created on 2015年11月10日

@author: Administrator
'''
import time
from datetime import datetime

from etl.util.bearychat import new_send_message
from etl.conf.settings import AuditConfig as Config

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
        self.title = u"小金汇报-android 手机"
        self.channel = u'广告-数据'
        self.normal = '#F8F8FF'
        self.error = '#FF0000'
        self.success = '#7FFFD4'

    def report(self):
        m_title = u'%s 数据审计完成' % datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        content = u"总共%d纪录, 发现%d行错误,共耗时%d秒\r\n" % (self.total, self.problems.get('total', 0), self.spent)
        
        column_total = self.problems["column_total"]
        for k,v in column_total.items():
            content += u"字段: %s, 错误次数: %d\r\n" % (k, v)
        sample = self.problems['sample']

        content += u"错误抽样样本%d条\r\n" % len(sample)

        for i in sample:
            content += i+"\r\n"

        if self.problems.get('total', 0) > 0:
            new_send_message(self.title , True, self.channel, m_title, content, self.error)
        else:
            new_send_message(self.title , True, self.channel, m_title, content, self.success)
            
class AdMonitor_audit:
    def __init__(self,filepath):
        self.filepath = filepath
        self.error_rows = 0
        self.count_rows = 0
        self.sample_error_size = Config["sample_error_size"]
        self.header = Config["header"]
        self.problems = []
        self.columns_errors = {}
        self.spent = 0
        
    def audit(self):
        start_time = time.time()
        with open(self.filepath,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                row=[i.strip() for i in line.strip().split(Config["file_split"])]
        #                 print row
                self.count_rows = self.count_rows + 1
                self.validator(row,self.count_rows)
        end_time = time.time()
        self.spent = end_time - start_time
                
    def report(self):
        sample = []
        max_s = self.sample_error_size
        if self.error_rows < self.sample_error_size:
            max_s = self.error_rows
        for i in range(0,max_s):
            sample.append(self.problems[i])
            
        total_problems = {'column_total': self.columns_errors,'total': len(self.problems), 'sample': sample}
        robot = ADMonitorAuditRobot(self.count_rows, total_problems, self.spent)
        robot.report()
            
    def validator(self,row,index):
        result = self.validate_field(index,row,Config["ip"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["ad_event_type"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["url"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["video_id"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["playlist_id"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["board_id"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["request_res"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["ad_list"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["time_delay"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["request_str"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["slot_id"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["mediabuy_id"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["creator_id"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["video_play_time"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["play_event"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["pf"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["device_id"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["uid"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["os"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["net"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["manufacturer"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["model"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["app"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["timestamp"])
        if not result:
            return False
        result = self.validate_field(index,row,Config["session_id"])
        if not result:
            return False
        
    def validate_field(self,index,row,field_name):       
        _index = self.header.index(field_name)
#         print row
        value = row[_index]
        scenes = Config["field_rule"][field_name]
        result = self._validate_value(row,value, scenes)
        if result == SUCCESS:
            return True
        self.error_rows = self.error_rows + 1
        problem = "行%s,列：%s 值：%s 错误信息：%s"%(str(index),field_name,value,result)
        self.problems.append(problem)
        c = self.columns_errors.get(field_name,0)
        c = c + 1
        self.columns_errors.update({field_name:c})
        return False
        
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
    
def ad_audit(filepath):
    filepath = r"C:\Users\Administrator\Desktop\ad_test_xx.csv"
    ad = AdMonitor_audit(filepath)
    ad.audit()
    ad.report()
        
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
