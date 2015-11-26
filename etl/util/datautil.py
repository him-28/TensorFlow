# encoding=utf8
''' merge the files '''
import os

import yaml
import pandas as pd

from etl.util import init_log
from etl.util import path_chk_or_create
from etl.logic1.ad_transform_pandas import split_header
from etl.conf.settings import MONITOR_CONFIGS as CNF

LOG = init_log.init("util/logger.conf", 'admonitorLogger')

def req_parser(dataframe):
    #TODO: 转化两个地址下不同nginx日志到目标文件
    #extract request, body fileds
    ad_res_parser = ""
    ad_event_parser = ""
    return None

def transform_ngx_log(ngx_path, ngx_filename, path, filename):
    """
    nginx log 配置:
    log_format post_tracking '$remote_addr^A$http_x_forwarded_for^A$host^A[$time_local]^A'
                             '$request_time^A$http_referer^A$http_user_agent^A^A$server_addr^A$request_length^A'
                             '$status^A$request^A$request_body';

    示例：
    curl http://centos/test\?a\=3\&b\=4 -v -d 'mac=dd:dd:ff:dd:dd&d=dfadfasdfas&p=s'
    log:
    10.211.55.2^A-^Acentos^A06/Sep/2015:06:25:31 -0400^A0.001^A-^Acurl/7.43.0^A-^A10.211.55.4^A188^A200^APOST /test?a=3&b=4 HTTP/1.1^Amac=dd:dd:ff:dd:dd&d=dfadfasdfas&p=s
    """
    f = os.path.join(ngx_path, ngx_filename)

    assert os.path.exists(f)
    
    ngx_header = ['remote_addr', 'http_x_forwarded_for', 'host', 'time_local', 'request_time' \
            'http_referer', 'http_user_agent', 'server_addr', 'request_length', 'status', \
            'request', 'request_body']

    dst_header = []

    # read file from nginx path

    df = pd.read_csv(names=ngx_header, sep="^A", encoding="utf-8", header=False)
    df1 = req_parser(df)

    #Store new columns file
    t_f = os.path.join(path, filename)
    path_chk_or_create(t_f)
    df1.to_csv(t_f, index=False, sep="\t", na_rep=" ", header=True)

def merge_file(input_paths, output_files):
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

    fun_info = "\nmerge file with params: \n%s\n%s" % (str(input_paths), str(output_files))
    LOG.info(fun_info)

    assert isinstance(input_paths, dict)
    assert isinstance(output_files, dict)

    df1 = None
    df2 = None
    df3 = None

    dtype = split_header(CNF.get("header_type"))

    for metric, input_list in input_paths.iteritems():

        LOG.info("merge metric: %s" % metric)
        assert isinstance(input_list, list)
        output_filename = output_files.get(metric)
        assert isinstance(output_filename, str)

        if os.path.exists(output_filename):
            os.remove(output_filename)
            LOG.warn("output file exists, remove")

        output_column_sep = CNF.get("output_column_sep")
        for i in range(1, len(input_list)):
            if i == 1:
                LOG.info("load file: %s " % input_list[0])
                df1 = pd.read_csv(input_list[0], sep=output_column_sep, \
                                  encoding="utf8", index_col=False, dtype=dtype)
            LOG.info("merge file: %s " % input_list[i])
            df2 = pd.read_csv(input_list[i], sep=output_column_sep, \
                               encoding="utf8", index_col=False, dtype=dtype)
            df3 = pd.concat([df1, df2])

            del df1, df2
            df1 = df3
            del df3

        LOG.info("sum merged datas")
        _group_item = CNF["algorithm"][metric]["group_item"]
        for _gi in _group_item:
            if _gi.startswith("query-"):
                 _group_item[_group_item.index(_gi)] = _gi.replace("query-","")
        df1 = df1.groupby(_group_item).sum()
        df1.to_csv(output_filename, sep=output_column_sep, na_rep=CNF.get("na_rep"), header=True)

