# encoding=utf-8
'''
Created on 2015年10月13日

@author: dico
'''
from etl.logic1.etl_transform_pandas import Etl_Transform_Pandas
import sys

def run(start_time,is_merge,is_old,is_console_print):
    '''run'''
    etp = Etl_Transform_Pandas(is_merge, is_console_print)
    if is_old:
        result1 = etp.compute_old('supply_day_hit', start_time)
        if result1 == -1:
            sys.exit(-1)
        result2 = etp.compute_old('supply_day_reqs', start_time)
        if result2 == -1:
            sys.exit(-1)
        result3 = etp.compute_old('demand_day_ad', start_time)
        if result3 == -1:
            sys.exit(-1)
    else:
        result1 = etp.compute('supply_day_hit', start_time)
        if result1 == -1:
            sys.exit(-1)
        result2 = etp.compute('supply_day_reqs', start_time)
        if result2 == -1:
            sys.exit(-1)
        result3 = etp.compute('demand_day_ad', start_time)
        if result3 == -1:
            sys.exit(-1)

if __name__ == '__main__':
    START_TIME = sys.argv[1]
    IS_MERGE = True
    if len(sys.argv) > 2:
        IS_MERGE = sys.argv[2] == 'merge'
    IS_CONSOLE_PRINT = False
    IS_OLD = False
    if len(sys.argv) > 3:
        IS_OLD = sys.argv[3] == 'old'
    if len(sys.argv) > 4:
        IS_CONSOLE_PRINT = sys.argv[4] == 'True'
    run(START_TIME,IS_MERGE,IS_OLD,IS_CONSOLE_PRINT)

