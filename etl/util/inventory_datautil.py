# encoding=utf8
''' merge the files '''
import os
import time

import yaml
import pandas as pd

from etl.logic1.ad_transform_pandas import split_header
from etl.logic1.ad_calculate_inventory import insert
from etl.conf.settings import CURRENT_ENV
from etl.conf.settings import MONITOR_CONFIGS as CNF
from etl.util import init_log
LOG = init_log.init("util/logger.conf", 'inventoryLogger')
ENV_CONF = yaml.load(file("conf/inventory_monitor_config.yml"))
SCNF = ENV_CONF[CURRENT_ENV]["store"]

def merge_file(input_paths, output_files, data_date):
    """
    @param input_paths:
    {
        "metric1":["path1", "path2"],
        "metric2":["path1", "path2"]
    }
    @param output_files:
    {
        "metric1":"path1",
        "metric2":"path2"
    }
    """
    start = time.clock()
    fun_info = "\nmerge file with params: \n%s\n%s" % (str(input_paths), str(output_files))
    LOG.info(fun_info)

    assert isinstance(input_paths, dict)
    assert isinstance(output_files, dict)

    dtype = split_header(CNF.get("header_type"))

    for metric, input_list in input_paths.iteritems():

        LOG.info("merge metric: %s" , metric)
        assert isinstance(input_list, list)
        output_filename = output_files.get(metric)
        assert isinstance(output_filename, str)

        if os.path.exists(output_filename):
            os.remove(output_filename)
            LOG.warn("output file exists, remove")
        error_output_filename = output_filename + ".err"
        if os.path.exists(error_output_filename):
            os.remove(error_output_filename)
            LOG.warn("error output file exists, remove")

        output_column_sep = CNF.get("output_column_sep")
        daydata_dataframe = load_files(input_list, output_column_sep, dtype)

        LOG.info("sum merged datas")
        addon_item = SCNF["result_item"]
        daydata_dataframe["city_id"] = daydata_dataframe["city_id"].fillna("-1").astype(int)
        daydata_dataframe["server_timestamp"] = daydata_dataframe["server_timestamp"].astype(float)
        daydata_dataframe = filter_error_data(data_date, daydata_dataframe, \
                          error_output_filename, dtype, output_column_sep)
        daydata_dataframe = daydata_dataframe.groupby(addon_item, as_index=False).sum()
        daydata_dataframe.to_csv(output_filename, sep=output_column_sep, na_rep=CNF.get("na_rep"), \
                   dtype=dtype, header=True, index=False)
        LOG.info("merged result saved at : %s, insert to db...", output_filename)
        insert(daydata_dataframe)
        end = time.clock()
        spend_time = "%0.2f" % (end - start)
        return report_infos(daydata_dataframe, spend_time)

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
                                   na_rep=CNF.get("na_rep"), \
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
    readed = False
    for input_file in input_list:
        if readed:
            if os.path.exists(input_file):
                LOG.info("merge file: %s " , input_file)
                df2 = pd.read_csv(input_file, sep=output_column_sep, \
                               encoding="utf8", index_col=False, dtype=dtype)
                df3 = pd.concat([df1, df2])
            else:
                LOG.error("merge file did not exists:%s", input_file)
            df1 = df3
        else:
            if os.path.exists(input_file):
                LOG.info("load file: %s " , input_file)
                df1 = pd.read_csv(input_file, sep=output_column_sep, \
                              encoding="utf8", index_col=False, dtype=dtype)
                readed = True
            else:
                LOG.error("merge file did not exists:%s", input_file)
    return df1
