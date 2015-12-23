# encoding:utf-8

import sys
if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

import os
import time
from datetime import datetime
from datetime import timedelta

from etl.conf.settings import LOGGER

from etl.util.platform_datautil import merge_file
from etl.report.platform_reporter import PlatformReportor
from etl.calculate.etl_platform import ExtractTransformLoadPlatform

data_output_prefix = "/data6/platform"
D_Dir = "{prefix}{sep}{year}{sep}{month:02d}"
H_Dir = "{prefix}{sep}{year}{sep}{month:02d}{sep}{day:02d}"
H_Logic1_Filename = "platform_{hour:02d}.csv"
D_Logic1_Filename = "platform_{day:02d}.csv"

def xda_files(the_date, data_index):
    '''get xda files'''
    xda_file = "{sep}data{data_index}{sep}xda{sep}{year}{sep}{month:02d}{sep}{day:02d}{sep}\{hour:02d}.log".format(
                data_index=data_index,
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return xda_file

def yda_files(the_date, data_index):
    '''get ngx files'''
    yda_file = "{sep}data{data_index}{sep}yda{sep}{year}{sep}{month:02d}{sep}{day:02d}{sep}\{hour:02d}.log".format(
                data_index=data_index,
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return yda_file

def get_hour_da_files(the_date):
    '''get hour ngx files'''
    result = []
    result.append(xda_files(the_date, 2)) # 0 ~1
    result.append(yda_files(the_date, 2)) # 0 ~1
    
    """
    for i in range(2, 5):
        result.append(ngx_files(the_date, i, 0)) # 0 ~1
        result.append(ngx_files(the_date, i, 15)) # 0 ~15
        result.append(ngx_files(the_date, i, 30)) # 15 ~ 30
        result.append(ngx_files(the_date, i, 45)) # 30 ~ 45
    """
    return result

def get_hour_ngx_files(the_date):
    '''get hour ngx files'''
    result = []
    #result.append(ngx_files(the_date, 2, 0)) # 0 ~1
    for i in range(2, 5):
        result.append(ngx_files(the_date, i, 0)) # 0 ~1
        result.append(ngx_files(the_date, i, 15)) # 0 ~15
        result.append(ngx_files(the_date, i, 30)) # 15 ~ 30
        result.append(ngx_files(the_date, i, 45)) # 30 ~ 45
    return result

def get_type_result_out_file(the_date,type,num):
    '''result out file'''
    r_f = "{sep}data6{sep}platform{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}{type}_{hour:02d}_{num}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                type=type,
                hour=the_date.hour,
                num=num
            )
    return r_f

def get_result_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}platform{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}platform_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def get_display_poss_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}platform{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}platform_display_poss_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def get_display_sale_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}platform{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}platform_display_sale_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def get_impression_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}platform{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}platform_impression_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def get_impression_end_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}platform{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}platform_impression_end_{hour:02d}.csv".format(
                year=the_date.year,
                month=the_date.month,
                day=the_date.day,
                sep=os.sep,
                hour=the_date.hour
            )
    return r_f

def get_click_out_file(the_date):
    '''result out file'''
    r_f = "{sep}data6{sep}platform{sep}{year}{sep}{month:02d}{sep}\
{day:02d}{sep}platform_click_{hour:02d}.csv".format(
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
    output_filename1 = D_Logic1_Filename.format(metric="platform", day=now.day)

    for i in range(0, 24):
        filename1 = H_Logic1_Filename.format(hour=i, metric="platform")
        paths1.append(os.path.join(src_path, filename1))

    logic1_src_paths.update({"platform": paths1})

    logic1_output_paths.update({"platform": os.path.join(output_path, output_filename1)})

    paths.update({
        'logic1_src_paths': logic1_src_paths,
        'logic1_output_paths': logic1_output_paths
        })
    return paths


if __name__ == "__main__":
    if sys.argv[1] == 'h':
        now = datetime.now() - timedelta(hours=1)
        now = datetime(2015,12,13,4,1,1)
        ngx_files = get_hour_da_files(now)
        #ngx_files = [sys.argv[2]]
        #result_out_file = sys.argv[3]
        #/home/dingzheng/.platform_${prefix}_${year}${month}${day}${hour}${dash}
        #dash_mark_path = sys.argv[4]
        num = int(sys.argv[2])
        cnt = int(sys.argv[3])
        arr_result_out_file = []
        arr_result_out_done_file = []
        for i in range(1,cnt+1):
            arr_result_out_file.append(get_type_result_out_file(now,"platform",i))
            arr_result_out_done_file.append(get_type_result_out_file(now,"platform_done",i))
        #print arr_result_out_file
        #print arr_result_out_done_file
        cfg = {
               "start_time": time.mktime((now.year, now.month, now.day, now.hour, 0, 0, 0, 0, 0)),
               "end_time": time.mktime((now.year, now.month, now.day, now.hour + 1, 0, 0, 0, 0, 0)),
               "src_files" : ngx_files,
               #"result_out_file": result_out_file,
               "result_out_file": get_type_result_out_file(now,"platform",num),
               "result_out_done_file": get_type_result_out_file(now,"platform_done",num)
        }
        etli = ExtractTransformLoadPlatform(cfg)
        run_cfg = {
            #"display_poss": get_display_poss_out_file(now),
            "display_sale": get_type_result_out_file(now,"platform_display_sale",num),
            "impression": get_type_result_out_file(now,"platform_impression",num),
            #"impression_end": get_impression_end_out_file(now),
            "click": get_type_result_out_file(now,"platform_click",num)
        }
        merge_cfg = {
            "result_out_all_file": get_result_out_file(now),
            "result_out_file": arr_result_out_file,
            "result_out_done_file": arr_result_out_done_file
        }
        #print run_cfg
        #print cfg
        #sys.exit()
        """
        "display_poss": result_out_file + ".display_poss",
        "display_sale": result_out_file + ".display_sale",
        "impression": result_out_file + ".impression",
        "impression_end": result_out_file + ".impression_end",
        "click": result_out_file + ".click"
        """
        infos = etli.run(run_cfg,merge_cfg)
        #os.mknod(dash_mark_path)
        PlatformReportor().report_hour(now, infos)
    elif sys.argv[1] == 'd':
        now = datetime.now() - timedelta(days=1)
        now = datetime(2015,12,13,4,1,1)
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
        PlatformReportor().report_day(now, infos)
        PlatformReportor().report_pdf(infos, now.strftime("%Y%m%d"))
