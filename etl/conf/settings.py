# encoding=utf-8
'''
Created on 2015年10月16日

@author: Administrator
'''
import yaml
from etl.util import init_log
Config=yaml.load(file("conf/config.yml"))
AuditConfig=yaml.load(file("conf/audit_config.yml"))
LOGGER = init_log.init("util/logger.conf", 'petlLogger')
