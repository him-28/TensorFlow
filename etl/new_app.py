# encoding:utf-8
import sys
import os
import time
from datetime import datetime
from datetime import timedelta

if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

from etl.calculate.etl_inventory import ExtractTransformLoadInventory

def ngx_files(the_date, data_index, time2d):
    '''get ngx files'''
    ngx_file = "{sep}data{data_index}{sep}ngx{sep}{year}{sep}{month:02d}{sep}{day:02d}{sep}\
log.da.hunantv.com-access.log-{year}{month:02d}{day:02d}{hour:02d}{time2d}".format(
                data_index=data_index,
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour,
                time2d=time2d
            )
    return ngx_file

def get_hour_ngx_files(the_date):
    '''get hour ngx files'''
    result = []
    for i in range(3, 6):
        result.append(ngx_files(the_date, i, 0))
        result.append(ngx_files(the_date, i, 15))
        result.append(ngx_files(the_date, i, 30))
    return result

def get_result_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}inventory{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}inventory_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def get_display_poss_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}inventory{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}inventory_display_poss_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def get_display_sale_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}inventory{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}inventory_display_poss_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

if __name__ == "__main__":
    now = datetime.now() - timedelta(hours=1)
    ngx_files = get_hour_ngx_files(now)

    cfg = {
          "src_files" : ngx_files,
          "result_out_file": get_result_out_file(now),
    }
    etli = ExtractTransformLoadInventory(cfg)
    run_cfg = {
        "display_poss": get_display_poss_out_file(now),
        "display_sale": get_display_sale_out_file(now)
    }
    etli.run(run_cfg)
