#encoding=utf-8

import pandas as pd

def transform_ngx_log(ngx_path, ngx_filename, path, filename):
    pass

def merge_file(filepath={}):
    """
    {
        "metric1": ["path1", "path2"],
        "metric2": ["path1", "path2"]
    }
    """
    assert type(filepath) == dict

    df1 = None
    df2 = None
    df3 = None

    for k, v in filepath.iteritems():

        assert type(v) == list

        for i in range(1, len(v)):
            if i == 1:
                df1 = pd.read_csv(v[0], encoding="utf-8")
            df2 = pd.read_csv(v[i], encoding="utf-8")
            df3 = pd.concat([df1, df2])

            del df1, df2
            df1 = df3

            del df3

