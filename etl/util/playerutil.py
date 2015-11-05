# encoding:utf-8
'''
Created on 2015年11月3日

@author: Administrator
'''
import datetime
import time

from etl.util.pgutil import DBUtils
from etl.conf.settings import LOGGER

PLAYER_TABLE_NAME = "ad_space"
GROUP_TABLE_NAME = ""
ADSPACE_TABLE_NAME = ""


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
    d = datetime.datetime.now()
    ftime=datetime.datetime.strftime(d,"%Y-%m-%d %H:%M")
    _PLAYERSQL = " select player_id,ad_id,node_id,name,priority,effect_s,effect_e from \"%s\" as s where s.status='1' and s.effect_e >= '%s'  order by player_id,node_id,priority asc" % \
                (PLAYER_TABLE_NAME,ftime)
    
    player_info = {}
    try:
        rows = DBUtils.fetchall(_PLAYERSQL)
        if not rows or not len(rows):
            return {}
        for row in rows:
            if not row or len(row) != 7:
                LOGGER.error("get player info row errors,should be 4 but value: %s" % row)
                continue
            _update_player_info(player_info, row)
        return player_info
    except Exception,e:
        LOGGER.error("get player info errors, message: %s" % e.message)
    return {}

def getGroupIdBySlotId(slotid):
    '''
        根据单个slotid获取groupid(不推荐使用，单个查询慢 . 推荐getAllGroupId() )
    '''
    if not slotid:
        return None
    
    d = datetime.datetime.now()
    ftime=datetime.datetime.strftime(d,"%Y-%m-%d %H:%M")
    _sql = "select group_id from \"%s\" as s where s.status='1' and s.slot_id=%s and s.effect_e >= '%s' " % (ADSPACE_TABLE_NAME,slotid,ftime)
    try:
        result = DBUtils.fetchone(_sql)
        if not result :
            return None
        return result
    except Exception,e:
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
        d=datetime.datetime.strptime(dtime,"%Y%m%d%H%M")
        
    ftime=datetime.datetime.strftime(d,"%Y-%m-%d %H:%M")
    _GROUPSQL = "select ad_id,node_id from \"%s\" as s where s.status='1' and s.effect_s <= '%s' and s.effect_e > '%s' " % (ADSPACE_TABLE_NAME,ftime,ftime)
    groups = {}
    try:
        rows = DBUtils.fetchall(_GROUPSQL)
        if not rows or not len(rows):
            return {}
        for row in rows:
            if not row or len(row) != 2:
                LOGGER.error("get  node_id ad_id relation row errors,should be 2 but value:" % row)
                continue
            groups[row[0]] = row[1]
        return groups
    except Exception,e:
        LOGGER.error("get player info errors, message: %s" % e.message)
    return {}

# def getAllGroupIdWithPlayerId():
    
    
    
def _update_player_info(playerinfo,row):
    _player=playerinfo.get(row[0])
    
    if _player is None:
        _player={}
        playerinfo[row[0]]=_player
    _slotid = _player.get(row[1])
    
    if _slotid is None:
        _slotid = []
        _player[row[1]]=_slotid
    _slotid.append(row[2])
    _slotid.append(row[3].decode("string_escape").encode('utf8'))
    _slotid.append(row[4])
    #TODO check
    ttps = row[5].timetuple()
    tsstart = time.mktime(ttps)
    ttpe = row[6].timetuple()
    tsend = time.mktime(ttpe)
    _slotid.append(tsstart)
    _slotid.append(tsend)
