#encoding = utf-8

import pandas as pd
import numpy as np


HEADER = ['ip', 'ty', 'ur', 'v', 'ci', 'b', 'rs', 'l', 'td', 'req', 's', 'cp', 'c', 'ct', 'o', 'e', 'pf', 'd', 'u', 'net', 'os', 'mf', 'mod', 'app', 'ts', 'si']

def mask(data):
    pass

def display_poss(data):
    data_res = data[ (data.ty=='e') & (data.pf=='010101') & (data.rs.isin([0,1])) ]
    print data_res
    return len(data_res)

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

def calc_ad_monitor(filename):
    df = pd.read_csv(filename, sep="\t", chunksize=10000000, index_col=False, encoding="utf-8", dtype={'pf': np.str})
    dp = 0
    d = 0
    im = 0
    ime = 0
    cl = 0
    for chunk in df:
        dp = dp + display_poss(chunk)
        d = d + display(chunk)
        im = im + impression(chunk)
        ime = ime + impression_end(chunk)
        cl = cl + click(chunk)

    print "%d, %d, %d, %d, %d" % (dp, d, im, ime, cl)

if __name__ == "__main__":
    calc_ad_monitor('../util/ad.csv')

