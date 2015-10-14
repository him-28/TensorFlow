# encoding=utf-8
'''
Created on 2015年10月13日

@author: dico
'''
from etl_transform_pandas import Etl_Transform_Pandas
import sys


if __name__ == '__main__':
    start_time = sys.argv[1]
    is_merge = True
    if len(sys.argv) > 2:
        is_merge = sys.argv[2] == 'merge'
    is_console_print = False
    if len(sys.argv) > 3:
        is_console_print = sys.argv[3] == 'True'
    etp = Etl_Transform_Pandas(is_merge,is_console_print)
    result1 = etp.compute('supply_day_hit', start_time)
    if result1 == -1:
        sys.exit(-1)
    result2 = etp.compute('supply_day_reqs', start_time)
    if result2 == -1:
        sys.exit(-1)
    result3 = etp.compute('demand_day_ad', start_time)
    if result3 == -1:
        sys.exit(-1)