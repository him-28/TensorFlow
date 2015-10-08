#encoding=utf-8

import pandas as pd
import petl as etl
import csv
import yaml

config = yaml.load(open("config.yml"))

SUPPLY_HEADER = config.get('supply').get('raw_header')

DEMAND_HEADER = config.get('demand').get('raw_header')

SUPPLY_AGG_HEADER = config.get('supply').get('agg_header')

DEMAND_AGG_HEADER = config.get('demand').get('agg_header')

CHUNK = 500
TMP_PATH = ""
COLUMN_SEP = config.get('column_sep')

def supply_extract(filename):
    supply_transform(read(filename, supply_header))

def demand_extract(filename):
    demand_transform(read(filename, demand_header))

def supply_transform(df):
    for chunk in df:
        chunk.describe().to_csv("describe.csv")
        tmp = chunk[SUPPLY_AGG_HEADER].groupby(SUPPLY_AGG_HEADER).size()
        tmp.to_csv('test.csv', mode="a", index=False, header=None)

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
    # localfile 
    # filename = '/Users/martin/Documents/03-raw data/20150923.04.product.demand.csv'
    # 10.100.5.82 file
    # filename = '/tmp/20150923.10.product.supply.csv'
    supply_extract(filename)

            
