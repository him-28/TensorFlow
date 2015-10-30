#encoding = utf-8

import os
import pandas as pd
import numpy as np

from datetime import datetime

__all__ = (
        'display_poss',
        'display',
        'impression',
        'impression_end',
        'click'
)

HEADER = ['ip', 'ty', 'ur', 'v', 'ci', 'b', 'rs', 'l', 'td', 'req', 's', 'cp', 'c', 'ct', 'o', 'e', 'pf', 'd', 'u', 'net', 'os', 'mf', 'mod', 'app', 'ts', 'si']

def mask(data):
    pass

def display_poss(data):
    #data_res = data[ (data.ty=='e') & (data.pf=='010101') & (data.rs.isin([0,1])) ]
    #print data_res
    #return len(data_res)

def display(data):
    data_res = data[ (data.ty=='e') & (data.pf=='010101') & (data.rs==0) ]
    print data_res
    return len(data_res)

def impression(data):
    data_res = data[ (data.ty=='p') & (data.e == 's') & (data.pf=='010101') ]
    print data_res
    return len(data_res)

def impression_end(data):
    data_res = data[ (data.ty=='p') & (data.e == 'e') & (data.pf=='010101') ]
    print data_res
    return len(data_res)

def click(data):
    data_res = data[ (data.ty=='p') & (data.e == 'c') & (data.pf=='010101') ]
    print data_res
    return len(data_res)

def to_tmp_csv(filename, data):
    """
    write new data to tmp csv
    """
    data.to_csv(filename, sep="\t", index=False)

H_DELAY = 10
D_DELAY = 1
W_DELAY = 7

def agg_hour_file(time):
    """
    聚合上一个小时的分钟数据
    """
    pre_time = time.hour - 1
    data = None

    for i in xrange(60, H_DELAY):
        path = "{prefix}/{year}/{month}/{day}/{hour}".format({
                prefix = settings.prefix,
                year = pre_time.year,
                month = pre_time.month,
                day = pre_time.day,
                hour = pre_time.hour
            })
        name = "{minute}_ad.csv".format(minute=i+H_DELAY)
        filename = os.path.join(path, name)
        df = pd.read_csv(filename, sep="\t", chunksize=10000000, index_col=False, encoding="utf-8", dtype={'pf': np.str})

        if data:
            data = df.concat(data, ignore_index=True)
        else:
            continue

    n_path = "{prefix}/{year}/{month}/{day}/{hour}_ad.csv".format({
            prefix = settings.prefix,
            year = time.year,
            month = time.month,
            day = time.day,
            hour = time.hour
            })

    pd.to_csv(n_path, sep="\t", index=False)
            


def agg_day_file(time):
    """
    聚合昨天每小时的数据
    """
    pre_time = time.day - 1
    data = None

    for i in xrange(24, D_DELAY):
        path = "{prefix}/{year}/{month}/{day}".format({
                prefix = settings.prefix,
                year = pre_time.year,
                month = pre_time.month,
                day = pre_time.day
            })
        name = "{hour}_ad.csv".format(hour=i+D_DELAY)
        filename = os.path.join(path, name)
        df = pd.read_csv(filename, sep="\t", chunksize=10000000, index_col=False, encoding="utf-8", dtype={'pf': np.str})

        if data:
            data = df.concat(data, ignore_index=True)
        else:
            continue

    n_path = "{prefix}/{year}/{month}/{day}_ad.csv".format({
            prefix = settings.prefix,
            year = time.year,
            month = time.month,
            day = time.day
            })

    pd.to_csv(n_path, sep="\t", index=False)


def calc_ad_monitor(path, filename):
    file_path = os.path.join(path, filename)
    df = pd.read_csv(filepath, sep="\t", chunksize=10000000, index_col=False, encoding="utf-8", dtype={'pf': np.str})
    dp = 0
    d = 0
    im = 0
    ime = 0
    cl = 0
    for chunk in df:
        group_df = chunk.groupby(['ty', 'e'])

        dp = dp + display_poss(chunk)
        d = d + display(chunk)
        im = im + impression(chunk)
        ime = ime + impression_end(chunk)
        cl = cl + click(chunk)

    print "%d, %d, %d, %d, %d" % (dp, d, im, ime, cl)

def transform_ngx_log(path, filename, out_path, out_filename):
    """
    nginx log format: xxx xxx xxx reqs
    """
    pass


class Runner:
    def run(self, time, mode='m'):
        """
        mode in minute, hour, day
        """
        assert type(time) == datetime
        assert mode in ['m', 'h', 'd']

        path = ""
        filename = ""

        if mode == 'm':
            path = "{prefix}/{year}/{month}/{day}/{hour}".format({
                    prefix = settings.prefix,
                    year = time.year,
                    month = time.month,
                    day = time.day,
                    hour = time.hour
                })
            filename = "{minute}_ad.csv".format(minute=time.minute)

            ngx_path = "{prefix}/{year}/{month}/{day}/{hour}".format({
                    prefix = settings.ngx_prefix,
                    year = time.year,
                    month = time.month,
                    day = time.day,
                    hour = time.hour
                })
            ngx_filename = "{minute}_ad.csv".format(minute=time.minute)

            transform_ngx_log(ngx_path, ngx_filename, path, filename)
        elif mode == 'h':
            path = "{prefix}/{year}/{month}/{day}".format({
                    prefix = settings.prefix,
                    year = time.year,
                    month = time.month,
                    day = time.day
                })
            filename = "{hour}_ad.csv".format(hour=time.hour)

        elif mode == 'd':
            path = "{prefix}/{year}/{month}".format({
                    prefix = settings.prefix,
                    year = time.year,
                    month = time.month
                })
            filename = "{day}_ad.csv".format(day=time.day)

        calc_ad_monitor(path, filename)

        if time.minute == 60:
            agg_hour_file(time)
        elif time.hour == 24:
            agg_day_file(time)

if __name__ == "__main__":
    calc_ad_monitor('../util/ad.csv')

