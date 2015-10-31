# encoding:utf-8
import sys
import os
import time
from datetime import datetime

if 'amble' not in sys.modules and __name__ == '__main__':
    import pythonpathsetter

from etl.conf.settings import LOGGER, Config
from etl.util.datautil import merge_file, transform_ngx_log
from etl.logic2.calc import calc_ad_monitor

from pdb import set_trace as st

METRICS = Config['metrics']

class AdMonitorRunner(object):

    def concat_output_path(self, path, num, minute):
        output_paths = {}
        for metric in METRICS:
            filename = "logic{num}_{metric}_ad_{minute}.csv".format(
                    num=num, 
                    metric=metric,
                    minute=minute)
            output_paths.update({ metric: os.path.join(path, filename)})

        return output_paths

    def _job_ready_by_minute(self, now):
        paths = {}
        ad_src_path = "{prefix}/{year}/{month}/{day}/{hour}".format(
                prefix = Config["data_prefix"],
                year = now.year,
                month = now.month,
                day = now.day,
                hour = now.hour
            )
        ad_src_filename = "ad_{minute}.csv".format(minute=now.minute)

        paths.update({
            'ad_src_path': ad_src_path,
            'ad_src_filename': ad_src_filename
            })

        ngx_src_path = "{prefix}/{year}/{month}/{day}/{hour}".format(
                prefix = Config["ngx_prefix"],
                year = now.year,
                month = now.month,
                day = now.day,
                hour = now.hour
            )
        ngx_src_filename = "ad_{minute}.csv".format(minute=now.minute)

        paths.update({
            'ngx_src_path': ngx_src_path,
            'ngx_src_filename': ngx_src_filename
            })


        ### logic0 path
        output_paths = self.concat_output_path(ad_src_path, 0, now.minute)

        paths.update({
            'logic0_output_paths': output_paths
            })

        ### logic1 path
        output_paths = self.concat_output_path(ad_src_path, 1, now.minute)

        paths.update({
            'logic1_output_paths': output_paths
            })

        ### logic2 path
        output_paths = self.concat_output_path(ad_src_path, 2, now.minute)

        paths.update({
            'logic2_output_paths': output_paths
            })

        return paths

    def _job_ready_by_hour(self, now):
        src_path = "{prefix}/{year}/{month}/{day}/{hour}".format(
            prefix = Config["data_prefix"],
            year = now.year,
            month = now.month,
            day = now.day,
            hour = now.hour - 1
        )
        output_path = "{prefix}/{year}/{month}/{day}".format(
            prefix = Config['data_prefix'],
            year = now.year,
            month = now.month,
            day = now. day
        )

        paths = {}
        logic0_src_paths = {}
        logic1_src_paths = {}
        logic0_output_paths = {}
        logic1_output_paths = {}

        for metric in METRICS:
            src_filename0 = "logic0_{metric}_ad_{minute}.csv"
            src_filename1 = "logic1_{metric}_ad_{minute}.csv"
            output_filename0 = "logic0_{metric}_ad_{hour}.csv".format(
                    metric = metric,
                    hour = now.hour
                    )
            output_filename1 = "logic1_{metric}_ad_{hour}.csv".format(
                    metric = metric,
                    hour = now.hour
                    )
            paths0 = []
            paths1 = []

            for i in xrange(10, 70, Config["h_delay"]):
                logic0_filename = src_filename0.format(minute=i, metric=metric)
                logic1_filename = src_filename1.format(minute=i, metric=metric)
                paths0.append(os.path.join(src_path, logic0_filename))
                paths1.append(os.path.join(src_path, logic1_filename))

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
        path = "{prefix}/{year}/{month}".format(
                prefix = Config["data_prefix"],
                year = now.year,
                month = now.month,
                day = now.day
            )

        paths = {}
        logic0_output_paths = {}
        logic1_output_paths = {}

        for metric in METRICS:
            filename0 = "{hour}_logic0_{metric}_ad.csv"
            filename1 = "{hour}_logic1_{metric}_ad.csv"
            paths0 = []
            paths1 = []

            for i in xrange(24, Config["d_delay"]):
                filename0.format(hour=i, metric=metric)
                filename1.format(hour=i, metric=metric)
                paths0.append(os.path.join(path, filename0))
                paths1.append(os.path.join(path, filename1))

            logic0_output_paths.update({metric: paths0})
            logic1_output_paths.update({metric: paths1})

        paths.update({
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

            #Transform nginx log
            start = time.clock()
            transform_ngx_log(
                    paths['ngx_src_path'], 
                    paths['ngx_src_filename'], 
                    paths['ad_src_path'],
                    paths['ad_src_filename'])
            end = time.clock()
            LOGGER.info("transform ngx log spent: %f s" % (end-start))

            #Calc File
            start = time.clock()
            # TODO: logic0
            #
            end = time.clock()
            LOGGER.info("logic0 calc spent: %f s" % (end-start))

            start = time.clock()
            # TODO: logic1
            #
            end = time.clock()
            LOGGER.info("logic1 calc spent: %f s" % (end-start))

            # logic2 code
            start = time.clock()
            calc_ad_monitor(
                    paths['ad_src_path'],
                    paths['ad_src_filename'],
                    paths['logic0_output_paths'])
            end = time.clock()
            LOGGER.info("logic2 calc spent: %f s" % (end-start))

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
            LOGGER.info("logic0 hour agg spent: %f s" % (end-start))

            # logic1 code
            start = time.clock()
            merge_file(paths['logic1_src_paths'], paths['logic1_output_paths'])
            end = time.clock()
            LOGGER.info("logic1 hour agg spend: %f s" % (end-start))

        elif mode == 'd':
            paths = self._job_ready_by_day(now)

            LOGGER.info("Job day paths: %s " % paths)

            # logic0 code
            start = time.clock()
            merge_file(paths['logic0_output_paths'])
            end = time.clock()
            LOGGER.info("logic0 hour agg spend: %f s" % (end-start))

            # logic1 code
            start = time.clock()
            merge_file(paths['logic1_output_paths'])
            end = time.clock()
            LOGGER.info("logic1 hour agg spend: %f s" % (end-start))


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
    except Exception,e:
        LOGGER.error("run app error,error message:"+ str(e))
        sys.exit(-1)


if __name__ == '__main__':
    '''
    args: python app.py ad_monitor m|h|d
    '''
    run_cli(sys.argv)
    
