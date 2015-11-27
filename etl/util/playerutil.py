# encoding:utf-8
'''
Created on 2015年11月3日

@author: Administrator
'''
import datetime
import time
import yaml

#import psycopg2 as psy
import MySQLdb as psy

from etl.util.pgutil import DBUtils
from etl.conf.settings import LOGGER

PLAYER_TABLE_NAME = "ad_space"
GROUP_TABLE_NAME = ""
ADSPACE_TABLE_NAME = ""

Config = yaml.load(file("conf/config.yml"))
config = {}
config.update({
    'database': Config['database']['db_name'],
    'user': Config['database']['user'],
    'password': Config['database']['password'],
    'host': Config['database']['host'],
    'port': Config['database']['port']
    })

def getplayerInfo():
    '''
        get latest player info
        {playerid:
            {
                slotid1:[node_id,name,seq,effect_s,effect_e],
                slotid2:[node_id,name,seq,effect_s,effect_e]
                ...
            }
        ...
        }
    '''
    ed = datetime.datetime.now()
    endtime = datetime.datetime.strftime(ed, "%Y-%m-%d %H:%M")
    starttime = getStartTime()
    
    _getsplittime_sql = " select unix_timestamp(effect_s) from %s where effect_s <='%s' and effect_e >= '%s' GROUP BY effect_s ORDER by effect_s asc" % \
                (PLAYER_TABLE_NAME, endtime, starttime)
    
    try:
        # splitrows = DBUtils.fetchall(_getsplittime_sql)
        _conn = None
        _cur = None
        try:
            _conn = psy.connect(db=config['database'], user=config['user'], \
                passwd=config['password'], host=config['host'], \
                port=config['port'])
            _cur = _conn.cursor()
            result  = _cur.execute(_getsplittime_sql)
            splitrows = _cur.fetchall()
        except Exception, e:
            LOGGER.error('pg bulk insert error: %s' % e)
            if _conn:
                _conn.rollback()
            return None
        finally:
            if _conn:
                _cur.close()
                _conn.close()
            del _conn
        if not splitrows or not len(splitrows):
            LOGGER.error("can't get player info time split info errors")
            raise Exception("can't get player info time split info errors")
        
        split_player_info = _getSplitInfo(splitrows, ed)
        return split_player_info
    except Exception, e:
        LOGGER.error("get player info errors, message: %s" % e.message)
    return {}

def getGroupIdBySlotId(slotid):
    '''
        根据单个slotid获取groupid(不推荐使用，单个查询慢 . 推荐getAllGroupId() )
    '''
    if not slotid:
        return None
    
    d = datetime.datetime.now()
    ftime = datetime.datetime.strftime(d, "%Y-%m-%d %H:%M")
    _sql = "select group_id from \"%s\" as s where s.status='1' and s.slot_id=%s and s.effect_e >= '%s' " % (ADSPACE_TABLE_NAME, slotid, ftime)
    try:
        result = DBUtils.fetchone(_sql)
        if not result :
            return None
        return result
    except Exception, e:
        LOGGER.error("get player info errors, message: %s" % e.message)
    return {}


def getAllGroupId(dtime=None):    
    '''
        dtime : %Y%m%d%H%M 201511041816
        get all groupid  slotid relation.  
    '''
    if dtime is None:
        d = datetime.datetime.now()
    else:
        d = datetime.datetime.strptime(dtime, "%Y%m%d%H%M")
        
    ftime = datetime.datetime.strftime(d, "%Y-%m-%d %H:%M")
    _GROUPSQL = "select ad_id,node_id,priority from %s as s where s.status='1' and s.effect_s <= '%s' and s.effect_e > '%s' " % (PLAYER_TABLE_NAME, ftime, ftime)
    groups = {}
    try:
        try:
            _conn = psy.connect(db=config['database'], user=config['user'],charset="utf8", \
                passwd=config['password'], host=config['host'], \
                port=config['port'])
            _cur = _conn.cursor()
            _cur.execute(_GROUPSQL)
            rows = _cur.fetchall()
        except Exception, e:
            LOGGER.error('get conn error: %s' % e)
            if _conn:
                _conn.rollback()
            return None
        finally:
            if _conn:
                _cur.close()
                _conn.close()
            del _conn
        if not rows or not len(rows):
            return {}
        for row in rows:
            if not row or len(row) != 3:
                LOGGER.error("get  node_id ad_id relation row errors,should be 3 but value:" % row)
                continue
            groups[str(row[0])] = [str(row[1]),str(row[2])]
        return groups
    except Exception, e:
        LOGGER.error("get player info errors, message: %s" % e.message)
    return {}

def getAllGroupName():
    '''
        dtime : %Y%m%d%H%M 201511041816
        get all groupid  name relation.  
    '''
    _GROUPSQL = "select pn.id,lt.name,pn.location_type from patch_node pn join location_type lt on pn.location_type=lt.id"
    groups = {}
    try:
        try:
            _conn = psy.connect(db=config['database'], user=config['user'], charset="utf8",\
                passwd=config['password'], host=config['host'], \
                port=config['port'])
            _cur = _conn.cursor()
            _cur.execute(_GROUPSQL)
            rows = _cur.fetchall()
        except Exception, e:
            LOGGER.error('get conn error: %s' % e)
            if _conn:
                _conn.rollback()
            return None
        finally:
            if _conn:
                _cur.close()
                _conn.close()
            del _conn
            
        if not rows or not len(rows):
            return {}
        for row in rows:
            if not row or len(row) != 3:
                LOGGER.error("get  node_id name relation row errors,should be 3 but value:" % row)
                continue
            groups[str(row[0])] = row[1]
        return groups
    except Exception, e:
        LOGGER.error("get player info errors, message: %s" % e.message)
    return {}
# def getAllGroupIdWithPlayerId():
    
def getStartTime():    
    ds = datetime.datetime.now().timetuple()
    l = time.mktime(ds)
    l = l - (60 * 60 * 24)
    startDt = datetime.datetime.fromtimestamp(l)
    starttime = datetime.datetime.strftime(startDt, "%Y-%m-%d %H:%M")
    return starttime
    
def _getSplitInfo(rows, endd):
    index = 0
    splitInfo = {}
    for row in rows:
        if index == 0:
            index = index + 1
            starttime = row[0]
            continue
        
        nextendtime = row[0]   
        splitplayerInfo = _getSplitPlayerInfo(starttime, nextendtime)
        splitInfo[index] = splitplayerInfo
        index = index + 1
        starttime = row[0]
        
    splitplayerInfo = _getSplitPlayerInfo(starttime, endd)
    splitInfo[index] = splitplayerInfo   
    return splitInfo
        
def _getSplitPlayerInfo(starttime, endtime):
    starttime = datetime.datetime.fromtimestamp(starttime)
    try:
        endtime = datetime.datetime.fromtimestamp(endtime)
    except:
        endtime = endtime
    starttimetuple = starttime.timetuple()
    endtimetuple = endtime.timetuple()
    l = time.mktime(starttimetuple)
    l = l + 1
    startDt = datetime.datetime.fromtimestamp(l)
    starttimestr = datetime.datetime.strftime(startDt, "%Y-%m-%d %H:%M:%S")  # +1 second    
    
    l = time.mktime(endtimetuple)
    l = l - 1
    endDt = datetime.datetime.fromtimestamp(l)
    endtimestr = datetime.datetime.strftime(endDt, "%Y-%m-%d %H:%M:%S")  #  -1 second
    starttimelong = time.mktime(starttimetuple)
    endtimelong = time.mktime(endtimetuple)
    
    _playersql = " select p.code,ad_id,node_id,s.name,priority from %s as s \
                join player p on s.player_id=p.id and p.online=1 and s.status='1' and \
             s.effect_s <= '%s' and  s.effect_e >= '%s'  order by s.player_id,node_id,priority asc" % \
             (PLAYER_TABLE_NAME, starttimestr, endtimestr)       

    try:
        _conn = psy.connect(db=config['database'],charset="utf8", user=config['user'], \
            passwd=config['password'], host=config['host'], \
            port=config['port'])
        _cur = _conn.cursor()
        _cur.execute(_playersql)
        rows = _cur.fetchall()
    except Exception, e:
        LOGGER.error('pg bulk insert error: %s' % e)
        if _conn:
            _conn.rollback()
        return None
    finally:
        if _conn:
            _cur.close()
            _conn.close()
        del _conn

    player_info = {}
    try:
        if not rows or not len(rows):
            return {}
        for row in rows:
            if not row or len(row) != 5:
                LOGGER.error("get player info row errors,should be 4 but value: %s" % row)
                continue
            _update_player_info(player_info, row)
        splitinfo = {}
        splitinfo["starttime"] = starttimelong
        splitinfo["endtime"] = endtimelong
        splitinfo["playerinfo"] = player_info
        return splitinfo
    except Exception, e:
        LOGGER.error("get player info errors, message: %s" % e.message)
    return {}

def _update_player_info(playerinfo, row):
    _player = playerinfo.get(row[0])
    #p.code,ad_id,node_id,s.name,priority
    if _player is None:
        _player = {}
        playerinfo[int(row[0])] = _player
    _slotid = _player.get(row[1])
    
    if _slotid is None:
        _slotid = []
        _player[int(row[1])] = _slotid
    _slotid.append(int(row[2]))
    _slotid.append(row[3])
    _slotid.append(int(row[4]))
