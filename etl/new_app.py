# encoding:utf-8

import sys
if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

import os
import time
from datetime import datetime
from datetime import timedelta

import logging.config
logging.config.fileConfig("calculate/logger.conf")
LOGGER = logging.getLogger('etlLogger')

from etl.calculate.inventory_datautil import merge_file
from etl.report.inventory_reporter import InventoryReportor
from etl.calculate.etl_time_inventory import ExtractTransformLoadTimeInventory

data_output_prefix = "/data6/inventory"
D_Dir = "{prefix}{sep}{year}{sep}{month:02d}"
H_Dir = "{prefix}{sep}{year}{sep}{month:02d}{sep}{day:02d}"
H_Logic1_Filename = "inventory_{metric}_{hour:02d}.csv"
D_Logic1_Filename = "inventory_{metric}_{day:02d}.csv"

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

def get_pv1_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}inventory{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}inventory_pv1_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def get_pv2_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}inventory{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}inventory_pv2_{hour:02d}.csv".format(
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
{day:02d}{sep}inventory_sale_{hour:02d}.csv".format(
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
    paths2 = []
    pathssale=[]

    for i in range(0, 24):
        pv1 = H_Logic1_Filename.format(hour=i, metric="pv1")
        pv2 = H_Logic1_Filename.format(hour=i, metric="pv2")
        display_sale = H_Logic1_Filename.format(hour=i, metric="sale")
        paths1.append(os.path.join(src_path, pv1))
        paths2.append(os.path.join(src_path, pv2))
        pathssale.append(os.path.join(src_path, display_sale))

    logic1_src_paths.update({"pv1": paths1,"pv2":paths2,"display_sale":pathssale})

    output_filename1 = D_Logic1_Filename.format(metric="pv1", day=now.day)
    output_filename2 = D_Logic1_Filename.format(metric="pv2", day=now.day)
    output_filenamesale = D_Logic1_Filename.format(metric="sale", day=now.day)

    logic1_output_paths.update({
            "pv1": os.path.join(output_path, output_filename1),
            "pv2": os.path.join(output_path, output_filename2),
            "display_sale": os.path.join(output_path, output_filenamesale)
    })

    paths.update({
        'logic1_src_paths': logic1_src_paths,
        'logic1_output_paths': logic1_output_paths
    })
    return paths


if __name__ == "__main__":
    if sys.argv[1] == 'h':
        now = datetime.now() - timedelta(hours=1)
        ngx_files = sys.argv[2].split(",")
        result_out_file = sys.argv[3]
        dash_mark_path = sys.argv[4].split(",")
        cfg = {
               "start_time": time.mktime((now.year, now.month, now.day, now.hour, 0, 0, 0, 0, 0)),
               "end_time": time.mktime((now.year, now.month, now.day, now.hour + 1, 0, 0, 0, 0, 0)),
               "src_files" : ngx_files,
               "result_out_file": result_out_file,
        }
        etli = ExtractTransformLoadTimeInventory(cfg)
        run_cfg = {
            "pv1": result_out_file + ".pv1",
            "pv2": result_out_file + ".pv2",
            "display_sale": result_out_file + ".sale"
        }
        infos = etli.run(run_cfg)

        for d_p in dash_mark_path:
            LOGGER.info("create touch: %s" % d_p)
            if not os.path.exists(d_p):
                os.mknod(d_p)

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
        infos = merge_file("pv1", paths['logic1_src_paths']["pv1"], paths['logic1_output_paths']["pv1"], now)
        infos = merge_file("pv2", paths['logic1_src_paths']["pv2"], paths['logic1_output_paths']["pv2"], now)
        infos = merge_file("display_sale", paths['logic1_src_paths']["display_sale"], paths['logic1_output_paths']["display_sale"], now)
        end = time.clock()
        LOGGER.info("merge file spend: %f s" % (end - start))
        #InventoryReportor().report_day(now, infos, channel="库存统计-天数据")
        #InventoryReportor().report_pdf(infos, now.strftime("%Y%m%d"))


