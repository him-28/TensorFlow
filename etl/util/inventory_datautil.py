# encoding=utf8
''' merge the files '''
import os

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

        readed = False
        for i in range(1, len(input_list)):
            if readed:
                if os.path.exists(input_list[i]):
                    LOG.info("merge file: %s " % input_list[i])
                    df2 = pd.read_csv(input_list[i], sep=output_column_sep, \
                                   encoding="utf8", index_col=False, dtype=dtype)
                    df3 = pd.concat([df1, df2])
                else:
                    LOG.error("merge file did not exists:%s",input_list[i])
                del df1, df2
                df1 = df3
                del df3
            else:
                if os.path.exists(input_list[i]):
                    LOG.info("load file: %s " % input_list[1])
                    df1 = pd.read_csv(input_list[i], sep=output_column_sep, \
                                  encoding="utf8", index_col=False, dtype=dtype)
                    readed = True
                else:
                    LOG.error("merge file did not exists:%s",input_list[i])

        LOG.info("sum merged datas")
        addon_item = SCNF["result_item"]
        df1["city_id"] = df1["city_id"].fillna("-1").astype(int)
        df1 = df1.groupby(addon_item, as_index=False).sum()
        df1.to_csv(output_filename, sep=output_column_sep, na_rep=CNF.get("na_rep"),\
                   dtype=dtype, header=True, index=False)
        LOG.info("merged result saved at : %s, insert to db...", output_filename)
        insert(output_filename)
