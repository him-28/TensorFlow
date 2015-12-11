'''
Created on 2015年12月11日

@author: Administrator
'''
import sys
import os
import pandas as pd

from etl.calculate.etl_inventory import CFG, split_header

if __name__ == '__main__':
    marks = sys.argv[1]
    for mark in marks.split(","):
        if not os.path.exists(mark):
            return
    result_files = sys.argv[2]
    result_path = sys.argv[3]

    dtype = split_header(CFG["dtype"])
    header = CFG["group_item"]["result_header"]

    dfs = None
    for r_f in result_files:
        if os.path.exists(r_f):
            df = pd.read_csv(r_f, sep=CFG["csv_sep"], dtype=dtype)
            if dfs is None:
                dfs = df
            else:
                dfs = dfs.append(df)
    dfs.groupby(header).sum().to_csv()

    for mark in marks.split(","):
        if not os.path.exists(mark):
            os.remove(mark)
