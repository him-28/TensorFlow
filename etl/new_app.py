# encoding:utf-8

import sys
if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

import os
import time
from datetime import datetime
from datetime import timedelta

from etl.conf.settings import LOGGER

from etl.util.inventory_datautil import merge_file
from etl.report.inventory_reporter import InventoryReportor
from etl.calculate.etl_inventory import ExtractTransformLoadInventory

data_output_prefix = "/data6/inventory"
D_Dir = "{prefix}{sep}{year}{sep}{month:02d}"
H_Dir = "{prefix}{sep}{year}{sep}{month:02d}{sep}{day:02d}"
H_Logic1_Filename = "inventory_{hour:02d}.csv"
D_Logic1_Filename = "inventory_{day:02d}.csv"

def ngx_files(the_date, data_index, time2d):
    '''get ngx files'''
    ngx_file = "{sep}data{data_index}{sep}ngx{sep}{year}{sep}{month:02d}{sep}{day:02d}{sep}\
log.da.hunantv.com-access.log-{year}{month:02d}{day:02d}{hour:02d}{time2d:02d}".format(
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
    for i in range(2, 5):
        result.append(ngx_files(the_date, i, 0))
        result.append(ngx_files(the_date, i, 15))
        result.append(ngx_files(the_date, i, 30))
        result.append(ngx_files(the_date, i, 45))
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
{day:02d}{sep}inventory_display_sale_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def _job_ready_by_day(now):
    output_path = D_Dir.format(prefix=data_output_prefix,
                            year=now.year,
                            sep=os.sep,
                            month=now.month)

    src_path = H_Dir.format(prefix=data_output_prefix,
                            year=now.year,
                            sep=os.sep,
                            month=now.month,
                            day=now.day)

    paths = {}
    logic1_src_paths = {}
    logic1_output_paths = {}

    paths1 = []
    output_filename1 = D_Logic1_Filename.format(metric="inventory", day=now.day)

    for i in range(0, 24):
        filename1 = H_Logic1_Filename.format(hour=i, metric="inventory")
        paths1.append(os.path.join(src_path, filename1))

    logic1_src_paths.update({"inventory": paths1})

    logic1_output_paths.update({"inventory": os.path.join(output_path, output_filename1)})

    paths.update({
        'logic1_src_paths': logic1_src_paths,
        'logic1_output_paths': logic1_output_paths
        })
    return paths


if __name__ == "__main__":
    if sys.argv[1] == 'h':
        now = datetime.now() - timedelta(hours=1)
        ngx_files = [sys.argv[2]]
        result_out_file = sys.argv[3]
        #/home/dingzheng/.inventory_${prefix}_${year}${month}${day}${hour}${dash}
        dash_mark_path = sys.argv[4]
        cfg = {
               "start_time": time.mktime((now.year, now.month, now.day, now.hour, 0, 0, 0, 0, 0)),
               "end_time": time.mktime((now.year, now.month, now.day, now.hour + 1, 0, 0, 0, 0, 0)),
               "src_files" : ngx_files,
               "result_out_file": result_out_file,
        }
        etli = ExtractTransformLoadInventory(cfg)
        run_cfg = {
            "display_poss": result_out_file + ".display_poss",
            "display_sale": result_out_file + ".display_sale"
        }
        infos = etli.run(run_cfg)
        os.mknod(dash_mark_path)
        #InventoryReportor().report_hour(now, infos, channel="库存统计-小时数据")
    elif sys.argv[1] == 'd':
        now = datetime.now() - timedelta(days=1)
        paths = _job_ready_by_day(now)

        LOGGER.info("Job hour paths: \r\n \
                logic1_src_paths: %s \r\n \
                logic1_output_path: %s \r\n \
                " % (paths['logic1_src_paths'],
                    paths['logic1_output_paths']))

        start = time.clock()
        # logic1 code
        infos = merge_file(paths['logic1_src_paths'], paths['logic1_output_paths'], now)
        end = time.clock()
        LOGGER.info("merge file spend: %f s" % (end - start))
        InventoryReportor().report_day(now, infos, channel="库存统计-天数据")
        InventoryReportor().report_pdf(infos, now.strftime("%Y%m%d"))


