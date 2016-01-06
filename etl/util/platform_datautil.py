# encoding=utf8
''' merge the files '''
import os
import time

import yaml
import pandas as pd

from etl.logic1.ad_transform_pandas import split_header
from etl.logic1.ad_calculate_platform import insert_day
from etl.conf.settings import CURRENT_ENV
from etl.conf.settings import MONITOR_CONFIGS as CNF
from etl.util import init_log
LOG = init_log.init("util/logger.conf", 'platformLogger')
ENV_CONF = yaml.load(file("conf/platform_monitor_config.yml"))
SCNF = ENV_CONF[CURRENT_ENV]["store"]

def merge_file_day(input_paths, output_files, data_date):
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

    load_type = {'year':'int',
                'month':'int',
                'day':'int',
                'hour':'int',
                'province_id':'string',
                'city_id':'string',
                'board_id':'string',
                'mediabuy_id':'string',
                'creator_id':'string',
                'slot_id':'string',
                'click':'int',
                'display_sale':'int',
                'impression':'int',
                'impression_end':'int',
		'display_poss':'int'
    }

    save_type = {'year':'int',
                'month':'int',
                'day':'int',
                'province_id':'string',
                'city_id':'string',
                'player_id':'string',
                'mediabuy_id':'string',
                'creative_id':'string',
                'slot_id':'string',
                'clicks':'int',
                'display':'int',
                'impressions':'int',
                'end_impressions':'int',
		'pv':'int'
    }

    load_dtype = split_header(load_type)
    save_dtype = split_header(save_type)

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
        daydata_dataframe = load_files(input_list, output_column_sep, load_dtype)

        LOG.info("sum merged datas")
        addon_item = SCNF["result_item_day"]
        daydata_dataframe = daydata_dataframe[daydata_dataframe["day"]==data_date.day]
        daydata_dataframe = daydata_dataframe.groupby(addon_item, as_index=False).sum()
        del daydata_dataframe["hour"]
        daydata_dataframe.to_csv(output_filename, sep=output_column_sep, na_rep=CNF.get("na_rep"), \
                   dtype=save_dtype, header=True, index=False)
        LOG.info("merged result saved at : %s, insert to db...", output_filename)
        daydata_dataframe['year'] = daydata_dataframe['year'].astype(int)
        daydata_dataframe['month'] = daydata_dataframe['month'].astype(int)
        daydata_dataframe['day'] = daydata_dataframe['day'].astype(int)
        insert_day(daydata_dataframe, data_date)
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
    impression = 0
    #impression_end = 0
    click = 0
    if not df1.empty:
        result_size = len(df1)
        display_sale = df1["display_sale"].sum()
        #display_poss = df1["display_poss"].sum()
        impression = df1["impression"].sum()
        #impression_end = df1["impression_end"].sum()
        click = df1["click"].sum()
    infos = {
         "result_size": result_size,
         "display_sale": display_sale,
         #"display_poss": display_poss,
         "spend_time": spend_time,
         "impression": impression,
         #"impression_end": impression_end,
         "click": click,
    }
    return infos

def load_files(input_list, output_column_sep, dtype):
    '''加载24个小时的文件到一个DataFrame里'''
    df1 = None
    df2 = None
    df3 = None
    readed = False
    for input_file in input_list:
        if readed:
            if os.path.exists(input_file):
                LOG.info("merge file: %s " , input_file)
                df2 = pd.read_csv(input_file, sep=output_column_sep, \
                               encoding="utf8", index_col=False, dtype=dtype)
                #df2['display_poss'] = 0
		df3 = pd.concat([df1, df2])
            else:
                LOG.error("merge file did not exists:%s", input_file)
            if not df3 is None and not df3.empty:
                df1 = df3
        else:
            if os.path.exists(input_file):
                LOG.info("load file: %s " , input_file)
                df1 = pd.read_csv(input_file, sep=output_column_sep, \
                              encoding="utf8", index_col=False, dtype=dtype)
		#df1['display_poss'] = 0
                readed = True
            else:
                LOG.error("merge file did not exists:%s", input_file)
    return df1
