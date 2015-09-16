# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
#    +Author wennu
#    +Email wenu@e.hunantv.com


"""
load data from mongodb to local *.csv 
"""

from conn_host import conn_coll
import sys
import datetime
import os
import re
import time
import uuid
from bson.errors import InvalidBSON

def getstr(line):
    if type(line) == unicode:
        return line
    elif type(line) == str:
        return line.decode('utf8','ignore')
    else:
        _ = str(line)
        return _.replace("ObjectId(", "").rstrip(")")


def fix_key(obj):
    for key in obj.keys():
        obj[key.replace('__', '_')] = obj[key]
        if key == "p_c_ip":
            obj["p_c_lct"] = obj[key]
    return obj

def load(coll, bid, day, hour, count):
    """
    count
    """

    coll_name = coll + '_' + bid + '_' + day + '_' + hour
    db_demand = conn_coll(coll_name)
    
    # fix decode error 
    query_filter = {'p.v.title':0,'p.v.keyword':0,'p.v.rname':0,'p.c.ua':0, 'p.v.hname':0, 'p.v.sub_type':0,'p.v.name':0}
    str_format = u'{slotid}\t{cardid}\t{creativeid}\t{deviceid}\t{type}\t{intime}\t{ip}\t{boardid}\t{_sid}\t{voideoid}\t{second}'
    defuat_dict = dict(map(lambda x:(x,'null'),re.findall('{(\S*?)}',str_format),))
    db_demand_save_path = "/home/wn/amble/etl/load/demand/{0}/{1}/{2}.{3}.{4}.demand.csv".format(bid, day[:6], day[:8], hour, count)
    dir = os.path.split(db_demand_save_path)[0]
    os.path.isdir(dir) and 1 or os.makedirs(dir)
    #print  db_demand_save_path
    
    rows = db_demand.find()
    print db_demand.count()
    with open(db_demand_save_path, 'wb') as fw:
	for row in rows:
	    new_obj = {}
	    new_obj.update(defuat_dict)
	    new_obj['bid'] = '6.0.1'
	    for k_1, v_1 in row.items():
		if type(v_1) == dict:
		    for k_2, v_2 in v_1.items():
			if type(v_2) == dict:
			    for k_3, v_3 in v_2.items():
				key = "{0}_{1}_{2}".format(k_1,k_2,k_3)
				new_obj[key] = getstr(v_3)
			else:
			    key = "{0}_{1}".format(k_1,k_2)
			    new_obj[key] = getstr(v_2)
		else:
		    #print k_1, v_1
		    new_obj[k_1] = getstr(v_1)
	    new_obj = fix_key(new_obj.copy())
	    new_obj.update(row)
	    wline = str_format.format(**new_obj).strip() + "\n"
	    fw.write(wline.encode('utf8','ignore'))

if __name__ == "__main__":
    coll = sys.argv[1]
    bid = sys.argv[2]
    day = sys.argv[3]
    hour = sys.argv[4]
    count  = sys.argv[5]
    
    import  logging
    import traceback
    try:
        load(coll, bid, day, hour, count)
    except Exception,e:
        logging.error(e.args)
        ex = traceback.format_exc()
        print 'this is e.printstack',ex
