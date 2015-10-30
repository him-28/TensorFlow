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

class AdMonitorRunner(object):

    def concat_output_path(self, prefix, path, metrics):
        output_paths = {}
        for metric in metrics:
            filename = "{prefix}_{metric}_ad.csv".format(prefix=prefix, metric=metric)
            output_paths.update({ metric: os.path.join(path, filename)})

        return output_paths


    def run(self, now, mode='m'):
        """
        mode in minute, hour, day
        """

        LOGGER.info("begin running etl job")

        assert type(now) == datetime
        assert mode in ['m', 'h', 'd']

        metrics = ['display_poss', 'display', 'impression', 'impression_end', 'click', 'up']

        if mode == 'm':

            ### transform nginx log
            path = "{prefix}/{year}/{month}/{day}/{hour}".format(
                    prefix = Config["data_prefix"],
                    year = now.year,
                    month = now.month,
                    day = now.day,
                    hour = now.hour
                )
            filename = "{minute}_ad.csv".format(minute=now.minute)

            ngx_path = "{prefix}/{year}/{month}/{day}/{hour}".format(
                    prefix = Config["ngx_prefix"],
                    year = now.year,
                    month = now.month,
                    day = now.day,
                    hour = now.hour
                )
            ngx_filename = "{minute}_ad.csv".format(minute=now.minute)

            start = time.clock()
            transform_ngx_log(ngx_path, ngx_filename, path, filename)
            end = time.clock()
            LOGGER.info("transform ngx log spend: %f s" % (end-start))

            ### calc ad csv
            ad_path = "{prefix}/{year}/{month}/{day}/{hour}".format(
                    prefix = Config["data_prefix"],
                    year = now.year,
                    month = now.month,
                    day = now.day,
                    hour = now.hour
                )

            st()
            #ad_filename = "{minute}_logic_{metric}_ad.csv".format(minute=now.minute)

            ### call logic0 code
            start = time.clock()
            ad_filename0_prefix = "{minute}_logic{num}".format(minute=now.minute, num=0)
            output_paths = self.concat_output_path(ad_filename0_prefix, ad_path, metrics)
            # TODO
            end = time.clock()
            LOGGER.info("logic0 calc spend: %f s" % (end-start))

            ### call logic1 code
            start = time.clock()
            ad_filename1_prefix = "{minute}_logic{num}".format(minute=now.minute, num=1)
            output_paths = self.concat_output_path(ad_filename1_prefix, ad_path, metrics)
            # TODO
            end = time.clock()
            LOGGER.info("logic1 calc spend: %f s" % (end-start))

            ### call logic2 code
            start = time.clock()
            ad_filename2_prefix = "{minute}_logic{num}".format(minute=now.minute, num=2)
            output_paths = self.concat_output_path(ad_filename2_prefix, ad_path, metrics)
            calc_ad_monitor(path, filename, output_paths)
            end = time.clock()
            LOGGER.info("logic2 calc spend: %f s" % (end-start))

        elif mode == 'h':
            path = "{prefix}/{year}/{month}/{day}/{hour}".format(
                    prefix = Config["data_prefix"],
                    year = now.year,
                    month = now.month,
                    day = now.day,
                    hour = now.hour
                )

            outputs_path0 = {}
            outputs_path1 = {}

            for metric in metrics:
                filename0 = "{minute}_logic0_{metric}_ad.csv"
                filename1 = "{minute}_logic1_{metric}_ad.csv"
                paths0 = []
                paths1 = []

                for i in xrange(60, Config["h_delay"]):
                    filename0.format(minute=i, metric=metric)
                    filename1.format(minute=i, metric=metric)
                    paths0.append(os.path.join(path, filename0))
                    paths1.append(os.path.join(path, filename1))

                outputs_path0.update({metric: paths0})
                outputs_path1.update({metric: paths1})

            start = time.clock()
            merge_file(outputs_path0)
            end = time.clock()
            LOGGER.info("logic0 hour agg spend: %f s" % (end-start))

            start = time.clock()
            merge_file(outputs_path1)
            end = time.clock()
            LOGGER.info("logic1 hour agg spend: %f s" % (end-start))

        elif mode == 'd':
            path = "{prefix}/{year}/{month}".format(
                    prefix = Config["data_prefix"],
                    year = now.year,
                    month = now.month,
                    day = now.day
                )

            outputs_path0 = {}
            outputs_path1 = {}

            for metric in metrics:
                filename0 = "{hour}_logic0_{metric}_ad.csv"
                filename1 = "{hour}_logic1_{metric}_ad.csv"
                paths0 = []
                paths1 = []

                for i in xrange(24, Config["d_delay"]):
                    filename0.format(hour=i, metric=metric)
                    filename1.format(hour=i, metric=metric)
                    paths0.append(os.path.join(path, filename0))
                    paths1.append(os.path.join(path, filename1))

                outputs_path0.update({metric: paths0})
                outputs_path1.update({metric: paths1})


            start = time.clock()
            merge_file(outputs_path0)
            end = time.clock()
            LOGGER.info("logic0 day agg spend: %f s" % (end-start))

            start = time.clock()
            merge_file(outputs_path1)
            end = time.clock()
            LOGGER.info("logic1 day agg spend: %f s" % (end-start))


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
    
