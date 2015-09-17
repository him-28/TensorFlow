# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
#    +Author wennu
#    +Email wenu@e.hunantv.com


"""
connect mongodb host
"""

from pymongo import MongoClient

"""
线上数据
10.100.2.32 主库
10.100.2.16 从库
10.100.2.9  从库

测试数据
10.100.2.77
"""

name = 'test'
def conn_db(database):
    if name == 'product':
	mhost =  '10.100.2.32'
    else:
	mhost = '10.100.2.77'

    client = MongoClient(mhost, 27017)
    db = client[database]

    return db
