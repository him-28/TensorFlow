# encoding:utf-8
import sys
import os
import time
from datetime import datetime
from datetime import timedelta

if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

from etl.conf.settings import LOGGER, Config
from etl.util import path_chk_or_create
from etl.util.datautil import merge_file, transform_ngx_log
#from etl.audit.admonitor_audit import main
from etl.logic2.calc import calc_ad_monitor
from etl.logic1.ad_transform_pandas import AdTransformPandas,buddha_bless_me
from etl.logic0.ad_etl_transform import calc_etl

#from etl.audit import admonitor_ad_audit
#from etl.util import admonitor_flat_data
from etl.report.reporter import DataReader
from etl.report.reporter import Reportor

METRICS = Config['metrics']
M_Dir = "{prefix}{sep}{year}{sep}{month}{sep}{day}{sep}{hour}"
H_Dir = "{prefix}{sep}{year}{sep}{month:02d}{sep}{day:02d}"
D_Dir = "{prefix}{sep}{year}{sep}{month:02d}"
M_Logic0_Filename = "logic0_{metric}_ad_{minute:02d}.csv"
H_Logic0_Filename = "logic0_{metric}_ad_{hour:02d}.csv"
D_Logic0_Filename = "logic0_{metric}_ad_{day:02d}.csv"
M_Logic1_Filename = "logic1_{metric}_ad_{minute:02d}.csv"
H_Logic1_Filename = "logic1_{metric}_ad_{hour:02d}.csv"
D_Logic1_Filename = "logic1_{metric}_ad_{day:02d}.csv"

class AdMonitorRunner(object):


    def concat_output_path(self, path, num, minute):
        output_paths = {}
        for metric in METRICS:
            filename = "logic{num}_{metric}_ad_{minute:02d}.csv".format(
                    num=num,
                    metric=metric,
                    minute=minute)

            tmp_path = os.path.join(path, filename)
            output_paths.update({ metric: tmp_path})

        return output_paths

    def _job_ready_by_minute(self, now):
        paths = {}
        ad_src_path = "{prefix}{sep}{year}{sep}{month}{sep}{day}{sep}{hour:02d}".format(
                prefix=Config["data_prefix"],
                year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour,
                sep=os.sep
            )

        path_chk_or_create(ad_src_path)

        ad_src_filename = "ad_{minute}.csv".format(minute=now.minute)

        paths.update({
            'ad_src_path': ad_src_path,
            'ad_src_filename': ad_src_filename
            })

        ngx_src_path = "{prefix}{sep}{year}{sep}{month}{sep}{day}{sep}{hour:02d}".format(
                prefix=Config["ngx_prefix"],
                year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour,
                sep=os.sep
            )
        ngx_src_filename = "ad_{minute}.csv".format(minute=now.minute)

        paths.update({
            'ngx_src_path': ngx_src_path,
            'ngx_src_filename': ngx_src_filename
            })


        # ## logic0 path
        output_paths = self.concat_output_path(ad_src_path, 0, now.minute)

        paths.update({
            'logic0_output_paths': output_paths
            })

        # ## logic1 path
        output_paths = self.concat_output_path(ad_src_path, 1, now.minute)

        paths.update({
            'logic1_output_paths': output_paths
            })

        # ## logic2 path
        output_paths = self.concat_output_path(ad_src_path, 2, now.minute)

        paths.update({
            'logic2_output_paths': output_paths
            })

        return paths

    def _job_ready_by_hour(self, now):
        '''计算当前小时'''
        paths = {}

        # Nginx 收集服务器存储的日志文件
        ngx_src_path = "{prefix}{sep}{year}{sep}{month:02d}{sep}{day:02d}".format(
                prefix=Config["ngx_prefix"],
                year=now.year,
                month=now.month,
                day=now.day,
                sep=os.sep
            )
        path_chk_or_create(ngx_src_path)

        ngx_src_filename = "ad_{hour:02d}.log".format(hour=now.hour)

        ad_src_path = "{prefix}{sep}{year}{sep}{month:02d}{sep}{day:02d}".format(
                prefix=Config["data_prefix"],
                year=now.year,
                month=now.month,
                day=now.day,
                sep=os.sep
            )
        path_chk_or_create(ad_src_path)
        ad_src_filename = "src_ad_{hour:02d}.csv".format(hour=now.hour)


        paths.update({
            'ngx_src_path': ngx_src_path,
            'ngx_src_filename': ngx_src_filename,
            'ad_src_path': ad_src_path,
            'ad_src_filename': ad_src_filename
            })

        output_paths = self.concat_output_path(ad_src_path, 0, now.hour)

        paths.update({
            'logic0_output_paths': output_paths
            })

        # ## logic1 path
        output_paths = self.concat_output_path(ad_src_path, 1, now.hour)

        paths.update({
            'logic1_output_paths': output_paths
            })
        return paths

    def _job_ready_by_day(self, now):
        output_path = D_Dir.format(prefix=Config["data_prefix"],
                                year=now.year,
                                sep=os.sep,
                                month=now.month)

        src_path = H_Dir.format(prefix=Config["data_prefix"],
                                year=now.year,
                                sep=os.sep,
                                month=now.month,
                                day=now.day)
        paths = {}
        logic0_src_paths = {}
        logic1_src_paths = {}
        logic0_output_paths = {}
        logic1_output_paths = {}

        for metric in METRICS:
            paths0 = []
            paths1 = []
            output_filename0 = D_Logic0_Filename.format(metric=metric, day=now.day)
            output_filename1 = D_Logic1_Filename.format(metric=metric, day=now.day)

            for i in xrange(0, 24, Config["d_delay"]):
                filename0 = H_Logic0_Filename.format(hour=i, metric=metric)
                filename1 = H_Logic1_Filename.format(hour=i, metric=metric)

                paths0.append(os.path.join(src_path, filename0))
                paths1.append(os.path.join(src_path, filename1))

            logic0_src_paths.update({metric: paths0})
            logic1_src_paths.update({metric: paths1})

            logic0_output_paths.update({metric: os.path.join(output_path, output_filename0)})
            logic1_output_paths.update({metric: os.path.join(output_path, output_filename1)})

        paths.update({
            'logic0_src_paths': logic0_src_paths,
            'logic1_src_paths': logic1_src_paths,
            'logic0_output_paths': logic0_output_paths,
            'logic1_output_paths': logic1_output_paths
            })
        return paths


    def run(self, now, mode='m'):
        """
        mode in minute, hour, day
        """

        LOGGER.info("begin running etl job")

        assert type(now) == datetime
        assert mode in ['m', 'h', 'd']

        if mode == 'm':
            paths = self._job_ready_by_minute(now)

            LOGGER.info("Job minutes paths: \r\n \
                    ngx_src_path: %s \r\n \
                    ngx_src_filename: %s \r\n \
                    ad_src_path: %s \r\n \
                    ad_src_filename: %s \r\n \
                    logic0_output_paths: %s \r\n \
                    logic1_output_paths: %s \r\n \
                    logic2_output_paths: %s \r\n" % (paths['ngx_src_path'],
                        paths['ngx_src_filename'],
                        paths['ad_src_path'],
                        paths['ad_src_filename'],
                        paths['logic0_output_paths'],
                        paths['logic1_output_paths'],
                        paths['logic2_output_paths']))

            # Transform nginx log
            start = time.clock()
            transform_ngx_log(
                    paths['ngx_src_path'],
                    paths['ngx_src_filename'],
                    paths['ad_src_path'],
                    paths['ad_src_filename'])
            end = time.clock()
            LOGGER.info("transform ngx log spent: %f s" % (end - start))

            # Calc File
            start = time.clock()
            #  logic0
            calc_etl(
                    paths['ad_src_path'],
                    paths['ad_src_filename'],
                    paths['logic0_output_paths'])
            end = time.clock()
            LOGGER.info("logic0 calc spent: %f s" % (end - start))

            start = time.clock()
            # logic1 code
            atp = AdTransformPandas()
            atp.calculate(
                    paths['ad_src_path'],
                    paths['ad_src_filename'],
                    paths['logic1_output_paths'])
            end = time.clock()
            LOGGER.info("logic1 calc spent: %f s" % (end - start))

            # logic2 code
            start = time.clock()
            calc_ad_monitor(
                    paths['ad_src_path'],
                    paths['ad_src_filename'],
                    paths['logic0_output_paths'])
            end = time.clock()
            LOGGER.info("logic2 calc spent: %f s" % (end - start))
            
            # load minute file in db
        elif mode == 'h':
            paths = self._job_ready_by_hour(now)

            LOGGER.info("Job hour paths: \r\n \
                    ngx_src_path: %s \r\n \
                    ngx_src_filename: %s \r\n \
                    ad_src_path: %s \r\n \
                    ad_src_filename: %s \r\n \
                    logic0_output_paths: %s \r\n \
                    logic1_output_paths: %s \r\n ",
                        paths['ngx_src_path'],
                        paths['ngx_src_filename'],
                        paths['ad_src_path'],
                        paths['ad_src_filename'],
                        paths['logic0_output_paths'],
                        paths['logic1_output_paths'])

            #ngx_src_path = os.path.join(paths["ngx_src_path"], paths["ngx_src_filename"])
            ad_src_path = os.path.join(paths["ad_src_path"], paths["ad_src_filename"])

            # 打平:
            #admonitor_flat_data.flat_data_admonitor(ngx_src_path, ad_src_path)

            # 审计:
            #admonitor_ad_audit.ad_audit(paths["ad_src_path"], paths["ad_src_filename"])

            # 计算
            start = time.clock()
            # logic0
#             calc_etl(
#                     paths['ad_src_path'],
#                     paths['ad_src_filename'],
#                     paths['logic0_output_paths'])
            end = time.clock()
            logic0_sptime = '%0.2f' % (end - start)
#             LOGGER.info("logic0 calc spent: %s s" , logic0_sptime)

            start = time.clock()
            # logic1 code
            atp = AdTransformPandas()
            atp.calculate(
                    paths['ad_src_path'],
                    paths['ad_src_filename'],
                    paths['logic1_output_paths'])
            end = time.clock()
            logic1_sptime = '%0.2f' % (end - start)
            LOGGER.info("logic1 calc spent: %s s" , logic1_sptime)

            # 读取计算结果
            d_reader = DataReader().hour_data(\
                                    paths['logic0_output_paths'], \
                                    paths['logic1_output_paths'])
            # 报告结果
            filesize = getfilesize(ad_src_path)
            params = {
                      "type":"hour",
                      "filename":paths["ad_src_filename"],
                      "filesize":filesize,
                      "logic0_sptime":logic0_sptime,
                      "logic1_sptime":logic1_sptime,
                      "start_time":now.strftime("%Y%m%d%H")}
            Reportor(params, d_reader).report_text()

        elif mode == 'd':
            paths = self._job_ready_by_day(now)

            LOGGER.info("Job hour paths: \r\n \
                    logic0_src_paths: %s \r\n \
                    logic1_src_paths: %s \r\n \
                    logic0_output_path: %s \r\n \
                    logic1_output_path: %s \r\n \
                    " % (paths['logic0_src_paths'],
                        paths['logic1_src_paths'],
                        paths['logic0_output_paths'],
                        paths['logic1_output_paths']))

            # logic0 code
            start = time.clock()
#             merge_file(paths['logic0_src_paths'], paths['logic0_output_paths'])

            # logic1 code
            merge_file(paths['logic1_src_paths'], paths['logic1_output_paths'])
            end = time.clock()
            sptime = '%0.2f' % (end - start)
            LOGGER.info("merge file spend: %f s" % (end - start))


            # 读取计算结果
            d_reader = DataReader().hour_data(\
                                    paths['logic0_output_paths'], \
                                    paths['logic1_output_paths'])

            params = {
                      "type":"day",
#                       "fileinfo0":getfilesinfo(paths['logic0_output_paths']),
                      "fileinfo1":getfilesinfo(paths['logic1_output_paths']),
                      "sptime":sptime,
                      "start_time":now.strftime("%Y%m%d")}
            # 报告结果
            Reportor(params, d_reader).report_text()

def getfilesize(filepath):
    psize = os.path.getsize(filepath)
    filesize = '%0.3f' % (psize / 1024.0 / 1024.0)
    return str(filesize) + "MB"

def getfilesinfo(filepaths):
    info = {}
    for key, filepath in filepaths.iteritems():
        filesize = getfilesize(filepath)
        filename = os.path.split(filepath)[1]
        info[key] = (filesize, filename)
    return info

def run_cli(arguments):
    try:
        run_type = arguments[1]
        args = arguments[2:]
        now = datetime.now()
        if run_type == 'admonitor':
            if 'h' == args[0]:
                now = now - timedelta(hours=1)
            elif 'd' == args[0]:
                now = now - timedelta(days=1)
            AdMonitorRunner().run(now, args[0])
        else:
            LOGGER.error("app run_type [{0}] is wrong".format(run_type))
            sys.exit(-1)
    except Exception, e:
        import traceback
        print traceback.format_exc()
        LOGGER.error("run app error,error message:" + str(e))
        sys.exit(-1)


if __name__ == '__main__':
    '''
    args: python app.py ad_monitor m|h|d
    '''
    buddha_bless_me()
    run_cli(sys.argv)

