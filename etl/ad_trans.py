#encoding=utf-8

import pandas as pd
import petl as etl
import csv

supply_header=['boardid','deviceid','videoid','slotid','cardid','creativeid','p_v_hid','p_v_rid','p_v_rname','p_c_type',
                'p_c_ip','cityid','intime','p_c_idfa1','p_c_imei','p_c_ctmid','p_c_mac','p_c_anid','p_c_openudid','p_c_idfa',
                'p_c_odin','p_c_aaid','p_c_duid','sid']

demand_header=[]

agg_header1 = ['slotid', 'cardid', 'creativeid']

CHUNK = 500
TMP_PATH = ""
COLUMN_SEP = "\t"

def supply_extract(filename):
    supply_transform(read(filename, supply_header))

def demand_extract(filename):
    demand_transform(read(filename, demand_header))

def supply_transform(df):
    for chunk in df:
        chunk.groupby(agg_header1).count()

def demand_transform(table):
    pass

def read(filename, header):
    return pd.read_csv(filename, names=header, sep=COLUMN_SEP, chunksize=CHUNK)

def load_to_tmp(table):
    pass

def load_to_pg_facts_hour(table):
    pass

def load_to_pg_facts_day(table):
    pass


if __name__ == '__main__':
    filename = '/Users/martin/Documents/03-raw data/20150923.04.product.demand.csv'
    supply_extract(filename)

            
