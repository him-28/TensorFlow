# encoding=utf-8
'''
Created on 2015年10月13日

@author: dico
'''
from etl_transform_pandas import Etl_Transform_Pandas
import sys

if __name__ == '__main__':
    date = sys.argv[1]
    hour = "%02d" % int(sys.argv[2])
    is_old = False
    if len(sys.argv) > 3:
        is_old = sys.argv[3] == 'old'
    is_console_print = False
    if len(sys.argv) > 4:
        is_console_print = sys.argv[4] == 'True'
    date_hour = date + "." + hour
    etp = Etl_Transform_Pandas(False, is_console_print)
    if is_old:
        result1 = etp.compute_old('supply_hour_hit', date_hour)
        if result1 == -1:
            sys.exit(-1)
        result2 = etp.compute_old('supply_hour_reqs', date_hour)
        if result2 == -1:
            sys.exit(-1)
        result3 = etp.compute_old('demand_hour_ad', date_hour)
        if result3 == -1:
            sys.exit(-1)
    else:
        result1 = etp.compute('supply_hour_hit', date_hour)
        if result1 == -1:
            sys.exit(-1)
        result2 = etp.compute('supply_hour_reqs', date_hour)
        if result2 == -1:
            sys.exit(-1)
        result3 = etp.compute('demand_hour_ad', date_hour)
        if result3 == -1:
            sys.exit(-1)