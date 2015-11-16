# encoding=utf-8
'''
Created on 2015年10月16日

@author: Administrator
'''
import yaml
from etl.util import init_log
Config = yaml.load(file("conf/config.yml"))
AuditConfig = yaml.load(file("conf/admonitor_audit_config.yml"))
APConfig=yaml.load(file("logic0/config.yml"))
FlatConfig = yaml.load(file("conf/flat_config.yml"))
ptLogger = init_log.init("util/logger.conf", 'petlLogger')
LOGGER = init_log.init("util/logger.conf", 'admonitorLogger')
PdLogger = init_log.init("util/logger.conf", 'pandasEtlLogger')


ENV_CONF = yaml.load(file("conf/monitor_config.yml"))
CURRENT_ENV = ENV_CONF.get("current_env")
MONITOR_CONFIGS = ENV_CONF[CURRENT_ENV]["monitor"]
AUDIT_HEADER = MONITOR_CONFIGS["audit_header"]
HEADER = MONITOR_CONFIGS["header"]
