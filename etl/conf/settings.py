# encoding=utf-8
'''
Created on 2015年10月16日

@author: Administrator
'''
import yaml
from etl.util import init_log
Config = yaml.load(file("conf/config.yml"))
AuditConfig = yaml.load(file("conf/audit_config.yml"))
APConfig=yaml.load(file("logic0/config.yml"))
LOGGER = init_log.init("util/logger.conf", 'petlLogger')

ENV_CONF = yaml.load(file("conf/monitor_config.yml"))
CURRENT_ENV = ENV_CONF.get("current_env")
MONITOR_CONFIGS = ENV_CONF[CURRENT_ENV]["monitor"]

