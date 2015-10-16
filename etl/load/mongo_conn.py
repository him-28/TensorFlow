# !/usr/bin/env python
# -*- coding: utf-8 -*-
#
#    +Author wennu
#    +Email wenu@e.hunantv.com


"""
load data from mongodb to local *.csv 
"""

from conn_host import conn_db
import sys
import string
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

    count = string.atoi(count)

    db = conn_db('emap')
    if coll == 'demand':
	coll_name = coll + '_' + bid + '_' + day + '_' + hour
	db_demand = db[coll_name]
	cnts = db_demand.count() / count + 1
	
	# fix decode error 
	#query_filter = {'p.v.rname':0, 'p.c.ua':0, 'p.v.hname':0, 'p.v.name':0}
	str_format = u'{slotid}\t{cardid}\t{creativeid}\t{deviceid}\t{type}\t{intime}\t{ip}\t{boardid}\t{_sid}\t{voideoid}\t{second}'
	default_dict = dict(map(lambda x:(x,'null'),re.findall('{(\S*?)}',str_format),))
	
	for cnt in range(0, cnts):
	    curr = cnt * count
	    
	    # local file path
	    db_demand_save_path = "./demand/{0}/{1}/{2}.{3}.{4}.demand.csv".format(bid, day[:6], day[:8], hour, curr)
	    dir = os.path.split(db_demand_save_path)[0]
	    os.path.isdir(dir) and 1 or os.makedirs(dir)
	    print db_demand_save_path
	    
	    # store by count
	    rows = db_demand.find().limit(count).skip(curr)
	    cnt += 1
	    with open(db_demand_save_path, 'wb') as fw:
		for row in rows:
		    new_obj = {}
		    new_obj.update(default_dict)
		    new_obj['sid'] = str(uuid.uuid4())
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
    else:
	coll_name = coll + '_' + bid + '_' + day + '_' + hour
	db_supply = db[coll_name]
	cnts = db_supply.count() / count + 1
	
	# fix decode error 
	#query_filter = {'p.v.rname':0,'p.c.ua':0, 'p.v.hname':0, 'p.v.name':0}
	str_format = u'{sid}\t{boardid}\t{deviceid}\t{voideoid}\t{slotid}\t{cardid}\t{creativeid}\t{intime}\t{p_c_os}\t{p_c_type}\t{p_c_ip}\t{p_v_hid}\t{p_v_rid}\t{p_v_rname}'
	default_dict = dict(map(lambda x:(x,'null'),re.findall('{(\S*?)}',str_format),))

	for cnt in range(0, cnts):
	    curr = cnt * count
	    
	    # local file path
	    db_supply_save_path = "./supply/{0}/{1}/{2}.{3}.{4}.supply.csv".format(bid, day[:6],day[:8], hour, curr)
	    dir = os.path.split(db_supply_save_path)[0]
	    os.path.isdir(dir) and 1 or os.makedirs(dir) #what the 1 meaning?  means if isdir  do noting
	    
	    print  db_supply_save_path
	    
	    # store by count
	    rows = db_supply.find().limit(count).skip(curr)
	    cnt += 1
	    with open(db_supply_save_path, 'wb') as fw:
		flag = True
		"""
		fix bson.errors.InvalidBSON
		"""
		n = 0
		for row in rows:
		    new_obj = {}
		    new_obj.update(default_dict)
		    new_obj['sid'] = str(uuid.uuid4())
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
			    new_obj[k_1] = getstr(v_1)
		    slotid = row.get("slotid",['-1',])
		    cardid = row.get("cardid",['-1',])
		    createiveid = row.get('creativeid',['-1',])
		    new_obj = fix_key(new_obj)
		    #print n, new_obj['sid'], zip(slotid,cardid, createiveid)
		    n += 1
		    for i in zip(slotid,cardid,createiveid):
			o = {}
			o['slotid'] = i[0]
			o['cardid'] = i[1]
			o['creativeid'] = i[2]
			new_obj.update(o)
			
			new_obj = fix_key(new_obj)
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
