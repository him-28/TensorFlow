# encoding=utf8
''' merge the files '''
import os
import time

import yaml
import pandas as pd

from etl.logic1.ad_transform_pandas import split_header
from etl.util import init_log
from etl.util.mysqlutil import DBUtils
from etl.logic1.ad_calculate_inventory import DataInsertPool, split_insert_sql, insert_data_frames
from etl.report.reporter import PF

LOG = init_log.init("util/logger.conf", 'inventoryLogger')
CONFIG_ALL = yaml.load(file("calculate/etl_time_conf.yml"))

CFG = CONFIG_ALL["inventory"]
REF = CONFIG_ALL["db_reflect"]


database = CFG["db_inventory"]
user = CFG["db_username"]
passwd = CFG["db_passwd"]
host = CFG["db_host"]
port = CFG["db_port"]

def insert(dataframe, trans_type, commit_size=30000):
    '''insert result to db'''
    LOG.info("insert result to db...")
    try:
        LOG.info("connect %s@%s:%s...", database, host, port)
        pool = DataInsertPool(\
                              DBUtils.get_connection(database, user, passwd, host, port), \
                              commit_size)
        dataframe = dataframe.groupby("pf")
        tablename = REF[trans_type]["table_name"]
        db_columns = REF[trans_type]["columns"].keys()

        for the_pf, datas in dataframe:
            if PF.has_key(the_pf):
                switch_table_name = tablename + the_pf
                pool.regist_sql(the_pf, \
                                split_insert_sql(db_columns, switch_table_name))
                datas.apply(insert_data_frames, axis=1, \
                            db_alias_info=REF[trans_type]["columns"], \
                            the_pf=the_pf, db_pool=pool, db_columns=db_columns)
                pool.flush(the_pf)
            else:
                LOG.error("unknow pf: %s", the_pf)
        pool.flush_all(is_close=True)
        LOG.info("insert job done.")
    except Exception, exc:
        LOG.error("insert error: %s ", exc)

def merge_file(trans_type, src_files, output_filename, data_date):
    start = time.clock()
    fun_info = "\nmerge [%s] file with params: \n%s\n%s" % \
        (trans_type, str(src_files), output_filename)
    LOG.info(fun_info)

    dtype = split_header(CFG["dtype"])

    if os.path.exists(output_filename):
        os.remove(output_filename)
        LOG.warn("output file exists:%s, remove", output_filename)
    error_output_filename = output_filename + ".err"
    if os.path.exists(error_output_filename):
        os.remove(error_output_filename)
        LOG.warn("error output file exists, remove")

    output_column_sep = CFG.get("csv_sep")
    daydata_dataframe = load_files(src_files, output_column_sep, dtype)

    LOG.info("sum merged datas")
    addon_item = REF[trans_type]["columns"].values()
    addon_item.remove(trans_type)
    addon_item.append("pf")
    daydata_dataframe["server_timestamp"] = daydata_dataframe["server_timestamp"].astype(float)
    daydata_dataframe = filter_error_data(data_date, daydata_dataframe, \
                      error_output_filename, dtype, output_column_sep)
    daydata_dataframe = daydata_dataframe.groupby(addon_item, as_index=False).sum()
    daydata_dataframe.to_csv(output_filename, sep=output_column_sep, \
               dtype=dtype, header=True, index=False)
    LOG.info("merged result saved at : %s, insert to db...", output_filename)
    insert(daydata_dataframe, trans_type)
    end = time.clock()
    spend_time = "%0.2f" % (end - start)
    #return report_infos(daydata_dataframe, spend_time)

def filter_error_data(data_date, daydata_dataframe, \
                      error_output_filename, dtype, output_column_sep):
    '''处理错误数据'''
    timestamp = time.mktime(data_date.date().timetuple())
    LOG.info("filter error timestamp datas [%s]...", timestamp)
    error_dataframe = daydata_dataframe[daydata_dataframe["server_timestamp"] != timestamp]
    if not error_dataframe.empty:
        err_len = len(error_dataframe)
        if err_len > 0:
            LOG.info("found [%s] error timestamp datas", err_len)
            error_dataframe.to_csv(error_output_filename, sep=output_column_sep, \
                                   dtype=dtype, header=True, index=False)
            LOG.info("save error datas to : %s", error_output_filename)
            return daydata_dataframe[daydata_dataframe["server_timestamp"] == timestamp]
    LOG.info("find no error timestamp data")
    return daydata_dataframe

def report_infos(df1, spend_time):
    '''返回结果报告'''
    result_size = 0
    display_sale = 0
    display_poss = 0
    details = {}
    if not df1.empty:
        result_size = len(df1)
        display_sale = df1["display_sale"].sum()
        display_poss = df1["display_poss"].sum()
        df2 = df1.groupby("pf").sum()
        for _pf, datas in df2.iterrows():
            details[_pf] = {
                "display_sale" : int(datas["display_sale"]),
                "display_poss" : int(datas["display_poss"])
           }
    infos = {
         "result_size": result_size,
         "display_sale": display_sale,
         "display_poss": display_poss,
         "spend_time": spend_time,
         "details": details
    }
    return infos

def load_files(input_list, output_column_sep, dtype):
    '''加载24个小时的文件到一个DataFrame里'''

    df1 = None
    for input_file in input_list:
        if not df1 is None:
            if os.path.exists(input_file):
                LOG.info("merge file: %s " , input_file)
                df2 = pd.read_csv(input_file, sep=output_column_sep, \
                               encoding="utf8", index_col=False, dtype=dtype)
                df1 = df1.append(df2)
                del df2
            else:
                LOG.error("merge file did not exists:%s", input_file)
        else:
            if os.path.exists(input_file):
                LOG.info("load file: %s " , input_file)
                df1 = pd.read_csv(input_file, sep=output_column_sep, \
                              encoding="utf8", index_col=False, dtype=dtype)
            else:
                LOG.error("merge file did not exists:%s", input_file)
    return df1
