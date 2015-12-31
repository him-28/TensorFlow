# encoding=utf8
'''
Created on 2015年12月11日

@author: Administrator
'''
import sys
if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

import os
import pandas as pd

from etl.conf.settings import LOGGER
from etl.report.reporter import PF,get_pf_name
from etl.report.inventory_reporter import InventoryReportor
from etl.util import bearychat as bc
from etl.calculate.etl_inventory import split_header
from etl.calculate.etl_time_inventory import CFG

def report(dataf, key, datetime_str):
    groups = dataf.groupby("pf")
    at_title = "%s 【%s】 指标统计" % (datetime_str, key)
    msg = ""
    for pf,datas in groups:
        if PF.has_key(pf):
            total = datas[key].sum ()
            pf_name = get_pf_name(pf)
            msg += "【%s】共计  %s " % pf_name, total
    bc.new_send_message(text="库存小进数据统计", at_title=at_title, \
                        channel="Test-Dico" , at_text=msg)


if __name__ == '__main__':
    marks = sys.argv[1]
    for mark in marks.split(","):
        if not os.path.exists(mark):
            sys.exit(0)

    for mark in marks.split(","):
        if os.path.exists(mark):
            os.remove(mark)

    result_files = sys.argv[2].split(",")
    result_path = sys.argv[3]
    tp = sys.argv[4]
    datatime_str = sys.argv[5]

    dtype = split_header(CFG["dtype"])
    header = CFG["group_item"][tp]
    if tp == "pv1":
        header.remove("board_id")
    dfs = None
    for r_f in result_files:
        if os.path.exists(r_f):
            df = pd.read_csv(r_f, sep=CFG["csv_sep"], dtype=dtype)
            if dfs is None:
                dfs = df
            else:
                dfs = dfs.append(df)
    result_df = pd.DataFrame(dfs.groupby(header, as_index=False).sum())
    LOGGER.info("save result to %s", result_path)
    result_df.to_csv(result_path, sep=CFG["csv_sep"], dtype=dtype, index=False)

    for r_f in result_files:
        if os.path.exists(r_f):
            os.remove(r_f)
    report(result_df, tp, datatime_str)
