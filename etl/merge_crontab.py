#encoding=utf8
'''
Created on 2015年12月11日

@author: Administrator
'''
import sys
if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

import os
import pandas as pd

from datetime import datetime, timedelta


from etl.conf.settings import LOGGER
from etl.report.inventory_reporter import InventoryReportor
from etl.calculate.etl_inventory import CFG, split_header

if __name__ == '__main__':
    marks = sys.argv[1]
    for mark in marks.split(","):
        if not os.path.exists(mark):
            sys.exit(0)
    result_files = sys.argv[2].split(",")
    result_path = sys.argv[3]

    dtype = split_header(CFG["dtype"])
    header = CFG["group_item"]["result_header"]
    header.append("pf")
    dfs = None
    for r_f in result_files:
        if os.path.exists(r_f):
            df = pd.read_csv(r_f, sep=CFG["csv_sep"], dtype=dtype)
            if dfs is None:
                dfs = df
            else:
                dfs = dfs.append(df)
    result_df = pd.DataFrame(dfs.groupby(header).sum())
    LOGGER.info("save result to %s", result_path)
    result_df.to_csv(result_path, sep=CFG["csv_sep"], dtype=dtype)

    for mark in marks.split(","):
        if not os.path.exists(mark):
            os.remove(mark)


    '''report result to BearyChat,Email'''
    result_size = 0
    display_sale = 0
    display_poss = 0
    if not result_df.empty:
        result_size = len(result_df)
        display_sale = result_df["display_sale"].sum()
        display_poss = result_df["display_poss"].sum()
    infos = {
         "file_name": "",
         "file_size": "",
         "result_size": result_size,
         "spend_time": "小于10分种",
         "display_sale": int(display_sale),
         "display_poss": int(display_poss)
    }
    details = {}
    if not result_df.empty:
        df2 = result_df.groupby("pf").sum()
        for the_pf, datas in df2.iterrows():
            details[the_pf] = {
                "display_sale" : int(datas["display_sale"]),
                "display_poss" : int(datas["display_poss"])
           }
    infos["details"] = details
    now = datetime.now() - timedelta(hours=1)
    InventoryReportor().report_hour(now, infos, channel="库存统计-小时数据")
