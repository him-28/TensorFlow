# encoding:utf-8
'''
Created on 2015年11月2日

@author: Administrator
'''
import os

from etl.util.pgutil import LoadUtils
from etl.conf.settings import Config
from etl.conf.settings import LOGGER

METRICS = Config['metrics']

def loadInDb_by_minute(paths):
    '''
                将产生的分钟计算文件，插入到DB中
    '''
    db_tablenames = Config["metrics_db_tablename_minute"]
    loadInDb(paths, db_tablenames)

def loadInDb_by_hour(paths):
    '''
                将产生的小时计算文件，插入到DB中
    '''
    db_tablenames = Config["metrics_db_tablename_hour"]
    loadInDb(paths, db_tablenames)
    
def loadInDb_by_day(paths):
    '''
                将产生的天计算文件，插入到DB中
    '''
    db_tablenames = Config["metrics_db_tablename_day"]
    loadInDb(paths, db_tablenames)
    
def loadInDb(paths,db_tablenames):
    if not paths:
        LOGGER.error("load  file error,paths is null")
        raise Exception("load file error,paths is null" )
    if not db_tablenames:
        LOGGER.error("load file error,tablenames is null")
        raise Exception("load file error,tablenames is null")
    for metric in METRICS:
        metric_filepath = paths.get(metric)
        checkFilePath(metric_filepath)
        tablename = db_tablenames.get(metric)
        LoadUtils.fromCsvtodb(metric_filepath,  tablename, commit=False,bulkread_size=1000, split_char=Config["calc_file_split"])

def checkFilePath(filepath):
    if not filepath or not os.path.exists(filepath):
        LOGGER.error("file not exists: %s" % filepath)
        raise Exception("file not exists: %s" % filepath)