# encoding: utf-8
'''
Created on 2015年11月11日

@author: Administrator
'''
import types
import sys
import os
import urllib

from etl.util.ip_convert import IP_Util
from etl.util.playerutil import getplayerInfo
from etl.conf.settings import FlatConfig as Config
from etl.conf.settings import LOGGER
from etl.conf.settings import HEADER
from etl.conf.settings import AUDIT_HEADER


ip_util=IP_Util(ipb_filepath=Config['ipb_filepath'],
                city_filepath=Config['city_filepath'])

player_info = getplayerInfo()

from pdb import set_trace as st

def flat_data_admonitor(input_path, output_path):

    #player_info = getplayerInfo()
    #time_range = get_time_range(player_info)
    #time_player_info = get_time_playerinfo(player_info)

    if os.path.exists(output_path):
        os.remove(output_path)

    buffers = []
    limit = 100000
    count = 0

    with open(input_path, 'rb') as f:
        while 1:
            lines = f.readlines(100000)
            if not lines:
                write_buffer(buffers, output_path)
                break

            for line in lines:
                #skip header
                count += 1
                if count == 1:
                    #append header
                    buffers.append(HEADER)
                    continue
                if len(buffers) > limit:
                    write_buffer(buffers, output_path)
                else:
                    new_lines = pack_data(line.strip('\n'))
                    buffers.extend(new_lines)

def write_buffer(buffers, output_path):
    with open(output_path, 'a') as f:
        for line in buffers:
            s = "\t".join(str(e) for e in line)
            f.write(s+"\n")

def get_area_info(ip):
    province = ip_util.get_cityInfo_from_ip(ip, 2)
    city = ip_util.get_cityInfo_from_ip(ip, 4)
    return province, city

def pack_data(line):
    delimiter = Config["file_split"]
    row = line.split(delimiter)
    #initial line
    new_data = []

    #TODO:check length
    if len(row) == len(AUDIT_HEADER):
        # gen province, city with ip
        ip_index = AUDIT_HEADER.index('ip')
        ip = row[ip_index]
        province, city = get_area_info(ip)
        row.append(province)
        row.append(city)

        # gen ad list
        adlist_index = AUDIT_HEADER.index('ad_list')
        ad_list = urllib.unquote(row[adlist_index])
        zip_data = map(lambda s: s.split(','), ad_list.split('|'))

        # gen seq, group_id
        seq = 1
        cur_group_id = ""

        for i in zip_data:
            new_row = row[:]
            # exists ad info
            if len(i) == 3:
                slotid_idx = AUDIT_HEADER.index('slot_id')
                mediabuyid_idx = AUDIT_HEADER.index('mediabuy_id')
                creativeid_idx = AUDIT_HEADER.index('creator_id')
                s_ts = AUDIT_HEADER.index('server_timestamp')
                b_id = AUDIT_HEADER.index('board_id')

                new_row[slotid_idx] = i[0]
                new_row[mediabuyid_idx] = i[1]
                new_row[creativeid_idx] = i[2]

                group_id = get_group_id(int(row[b_id]), int(i[0]), float(row[s_ts]))
                #FIXME: challenge

                new_row.append(seq)
                new_row.append(group_id)
                if group_id != cur_group_id:
                    seq = 1
                else:
                    seq += 1

                cur_group_id = group_id
            else:
                seq = ""
                group_id = ""
                new_row.append(seq)
                new_row.append(group_id)

            new_data.append(new_row)

        return new_data


def get_group_id(board_id, slotid, s_timestamp):
    '''
    {"slot_id+s_timestamp": "group_id"}
    '''
    group_id = ""
    try:
        for k, v in player_info.iteritems():
            start = v.get('starttime')
            end = v.get('endtime')
            if s_timestamp > start and s_timestamp < end:
                group_id = v['playerinfo'][board_id][slotid][0]
                break
    except Exception as e:
        LOGGER.error("groupid not found, board_id: %d, slotid:%d, s_timestamp:%f" % \
                (board_id, slotid, s_timestamp))

    return group_id
    
def get_time_range(allplayerinfo):
    '''
        return {1:[14967272327.0,14967343727.0],2:[14967272327.0,14967343727.0]}
    '''
    time_range = {}
    for k,v in allplayerinfo.items():
        time_range.update({k:[v.get("starttime"),v.get("endtime")]})
    return time_range

def get_time_playerinfo(allplayerinfo):
    '''
        return {1:{slotid:groupid,slotid1:groupid1 ...},2{slotid:groupid}}
    '''
    alltime_playerinfo = {}
    for k,v in allplayerinfo.items():
        playerinfo={}
        alltime_playerinfo[k] = playerinfo
        for ki,vi in v.get("playerinfo").items():
            for kii,vii in vi.items():
                playerinfo[kii] = vii[0] 
    return alltime_playerinfo
        
        
if __name__ == "__main__":
    inputf = "/Users/martin/Desktop/ad_13.log"
    output = "/Users/martin/Desktop/ad_flat.log"
    flat_data_admonitor(inputf,output)
