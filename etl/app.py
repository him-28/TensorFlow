# encoding:utf-8
import sys
import os
import time
from datetime import datetime

if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

from etl.conf.settings import LOGGER, Config
from etl.util import path_chk_or_create
from etl.util.datautil import merge_file, transform_ngx_log
from etl.audit.admonitor_audit import main
from etl.util.load_data import loadInDb_by_minute, loadInDb_by_hour, loadInDb_by_day
from etl.logic2.calc import calc_ad_monitor
from etl.logic1.ad_transform_pandas import AdTransformPandas
from etl.logic0.ad_etl_transform import calc_etl

from etl.audit import admonitor_ad_audit
from etl.util import admonitor_flat_data
from etl.report.reporter import DataReader
from etl.report.reporter import Reportor

METRICS = Config['metrics']
M_Dir = "{prefix}/{year}/{month}/{day}/{hour}"
H_Dir = "{prefix}/{year}/{month}/{day}"
D_Dir = "{prefix}/{year}/{month}"
M_Logic0_Filename = "logic0_{metric}_ad_{minute}.csv"
H_Logic0_Filename = "logic0_{metric}_ad_{hour}.csv"
D_Logic0_Filename = "logic0_{metric}_ad_{day}.csv"
M_Logic1_Filename = "logic1_{metric}_ad_{minute}.csv"
H_Logic1_Filename = "logic1_{metric}_ad_{hour}.csv"
D_Logic1_Filename = "logic1_{metric}_ad_{day}.csv"

class AdMonitorRunner(object):


    def concat_output_path(self, path, num, minute):
        output_paths = {}
        for metric in METRICS:
            filename = "logic{num}_{metric}_ad_{minute}.csv".format(
                    num=num,
                    metric=metric,
                    minute=minute)

            tmp_path = os.path.join(path, filename)
            output_paths.update({ metric: tmp_path})

        return output_paths

    def _job_ready_by_minute(self, now):
        paths = {}
        ad_src_path = "{prefix}/{year}/{month}/{day}/{hour}".format(
                prefix=Config["data_prefix"],
                year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour
            )

        path_chk_or_create(ad_src_path)

        ad_src_filename = "ad_{minute}.csv".format(minute=now.minute)

        paths.update({
            'ad_src_path': ad_src_path,
            'ad_src_filename': ad_src_filename
            })

        ngx_src_path = "{prefix}/{year}/{month}/{day}/{hour}".format(
                prefix=Config["ngx_prefix"],
                year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour
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

    def job_ready_by_chour(self, now):
        paths = {}
        ad_src_path = "{prefix}/{year}/{month}/{day}".format(
                prefix=Config["data_prefix"],
                year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour
            )

        path_chk_or_create(ad_src_path)

        ad_src_filename = "{year}{month}{day}.{hour}.csv".format(year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour)

        ad_src_flat_filename = "{year}{month}{day}.{hour}.flat.csv".format(year=now.year,
                month=now.month,
                day=now.day,
                hour=now.hour)

        paths.update({
            'ad_src_path': ad_src_path,
            'ad_src_filename': ad_src_filename,
            'ad_src_flat_filename': ad_src_flat_filename
            })

        # ## logic0 path
        output_paths = self.concat_output_path(ad_src_path, 0, now.hour)

        paths.update({
            'logic0_output_paths': output_paths
            })

        # ## logic1 path
        output_paths = self.concat_output_path(ad_src_path, 1, now.hour)

        paths.update({
            'logic1_output_paths': output_paths
            })

        # ## logic2 path
        output_paths = self.concat_output_path(ad_src_path, 2, now.hour)

        paths.update({
            'logic2_output_paths': output_paths
            })

        return paths

    def _job_ready_by_hour(self, now):
        output_path = H_Dir.format(prefix=Config["data_prefix"],
                                year=now.year,
                                month=now.month,
                                day=now.day)

        src_path = M_Dir.format(prefix=Config["data_prefix"],
                                year=now.year,
                                month=now.month,
                                day=now.day,
                                hour=now.hour - 1)

        paths = {}
        logic0_src_paths = {}
        logic1_src_paths = {}
        logic0_output_paths = {}
        logic1_output_paths = {}

        for metric in METRICS:
            paths0 = []
            paths1 = []
            output_filename0 = H_Logic0_Filename.format(metric=metric, hour=now.hour)
            output_filename1 = H_Logic1_Filename.format(metric=metric, hour=now.hour)

            for i in xrange(10, 70, Config["h_delay"]):
                filename0 = M_Logic0_Filename.format(minute=i, metric=metric)
                filename1 = M_Logic1_Filename.format(minute=i, metric=metric)

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
        
    def _job_ready_by_day(self, now):
        output_path = D_Dir.format(prefix=Config["data_prefix"],
                                year=now.year,
                                month=now.month)

        src_path = H_Dir.format(prefix=Config["data_prefix"],
                                year=now.year,
                                month=now.month,
                                day=now.day - 1)
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

            for i in xrange(1, 24, Config["d_delay"]):
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
        assert mode in ['m', 'h', 'ch', 'd']

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
            loadInDb_by_minute(paths["logic0_output_paths"])
            loadInDb_by_minute(paths["logic1_output_paths"])
        elif mode == 'h':
            paths = self._job_ready_by_hour(now)

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
            merge_file(paths['logic0_src_paths'], paths['logic0_output_paths'])
            end = time.clock()
            LOGGER.info("logic0 hour agg spent: %f s" % (end - start))

            # logic1 code
            start = time.clock()
            merge_file(paths['logic1_src_paths'], paths['logic1_output_paths'])
            end = time.clock()
            LOGGER.info("logic1 hour agg spend: %f s" % (end - start))
            
            # load hour file in db
            loadInDb_by_hour(paths["logic0_output_paths"])
            loadInDb_by_hour(paths["logic1_output_paths"])
        elif mode == 'ch':
            paths = self.job_ready_by_chour(now)

            LOGGER.info("Job cHour paths: \r\n \
                    ad_src_path: %s \r\n \
                    ad_src_filename: %s \r\n \
                    logic0_output_paths: %s \r\n \
                    logic1_output_paths: %s \r\n \
                    logic2_output_paths: %s \r\n" ,
                        paths['ad_src_path'],
                        paths['ad_src_filename'],
                        paths['logic0_output_paths'],
                        paths['logic1_output_paths'],
                        paths['logic2_output_paths'])
            ad_src_path = os.path.join(paths["ad_src_path"], paths["ad_src_filename"])
            ad_src_flat_path = os.path.join(paths["ad_src_path"], paths["ad_src_flat_filename"])

            #Audit data files:
            admonitor_ad_audit.ad_audit(ad_src_path)
            #Flat data files:
            admonitor_flat_data.flat_data(ad_src_path, ad_src_flat_path)

            # Calc File
            start = time.clock()
            #  logic0
            calc_etl(
                    paths['ad_src_path'],
                    paths['ad_src_flat_filename'],
                    paths['logic0_output_paths'])
            end = time.clock()
            LOGGER.info("logic0 calc spent: %f s" % (end - start))

            start = time.clock()
            # logic1 code
            atp = AdTransformPandas(True)
            atp.calculate(
                    paths['ad_src_path'],
                    paths['ad_src_flat_filename'],
                    paths['logic1_output_paths'])
            end = time.clock()
            LOGGER.info("logic1 calc spent: %f s" % (end-start))

            # report the result
            dr = DataReader().hour_data(paths['logic0_output_paths'], paths['logic1_output_paths'])
            Reportor(now.strftime("%Y-%m-%d.%H"), dr).report_text()

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
            merge_file(paths['logic0_src_paths'], paths['logic0_output_paths'])
            end = time.clock()
            LOGGER.info("logic0 hour agg spend: %f s" % (end - start))

            # logic1 code
            start = time.clock()
            merge_file(paths['logic1_src_paths'], paths['logic1_output_paths'])
            end = time.clock()
            LOGGER.info("logic1 hour agg spend: %f s" % (end - start))
            
            # load day file in db
            loadInDb_by_day(paths["logic0_output_paths"])
            loadInDb_by_day(paths["logic1_output_paths"])

def run_cli(arguments):
    try:
        run_type = arguments[1]
        args = arguments[2:]
        if run_type == 'admonitor':
            now = datetime.now()
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
    args: python app.py ad_monitor m|h|d|ch
    '''
    # 传入参数为：  ad_monitor ch
    try:
        run_cli(sys.argv)
    except:
        import traceback
        print traceback.format_exc()
    # main('util/ad.csv')
    
