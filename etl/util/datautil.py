# encoding=utf8
''' merge the files '''
import os

import pandas as pd

from etl.util import init_log
LOG = init_log.init("util/logger.conf", 'mergeLogger')

def transform_ngx_log(ngx_path, ngx_filename, path, filename):
    pass

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

    for metric, input_list in input_paths.iteritems():

        LOG.info("merge metric: %s" % metric)
        assert isinstance(input_list, list)
        output_filename = output_files.get(metric)
        assert isinstance(output_filename, str)

        if os.path.exists(output_filename):
            os.remove(output_filename)
            LOG.warn("output file exists, remove")

        for i in range(1, len(input_list)):
            if i == 1:
                LOG.info("load file: %s " % input_list[0])
                df1 = pd.read_csv(input_list[0], sep="\t", \
                                  encoding="utf8", header=None, index_col=False)
            LOG.info("merge file: %s " % input_list[i])
            df2 = pd.read_csv(input_list[i], sep="\t", \
                               encoding="utf8", header=None, index_col=False)
            df3 = pd.concat([df1, df2])

            del df1, df2
            df1 = df3
            del df3

        LOG.info("sum merged datas")
        _len = len(df1.columns) - 1
        _group_item = [x for x in range(0, _len)]
        df1 = df1.groupby(_group_item).sum()
        df1.to_csv(output_filename, sep="\t", na_rep=" ", header=False)

