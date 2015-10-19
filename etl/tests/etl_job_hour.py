# encoding=utf-8
'''
Created on 2015年10月13日

@author: dico
'''
from etl.logic1.etl_transform_pandas import Etl_Transform_Pandas
import sys

def run(the_date,the_hour, is_old, is_console_print):
    '''run '''
    date_hour = the_date + "." + the_hour
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

if __name__ == '__main__':
    THE_DATE = sys.argv[1]
    THE_HOUR = "%02d" % int(sys.argv[2])
    IS_OLD = False
    if len(sys.argv) > 3:
        IS_OLD = sys.argv[3] == 'old'
    IS_CONSOLE_PRINT = False
    if len(sys.argv) > 4:
        IS_CONSOLE_PRINT = sys.argv[4] == 'True'
    run(THE_DATE, THE_HOUR, IS_OLD, IS_CONSOLE_PRINT)
    