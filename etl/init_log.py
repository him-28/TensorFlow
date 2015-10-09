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
import logging
import logging.config

def init(log_config_path, log_handler_name):
    '''
    '''
    logging.config.fileConfig(log_config_path)
    logger = logging.getLogger(log_handler_name)
    return logger
