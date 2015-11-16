# encoding: utf-8
'''
Created on 2015年11月11日

@author: Administrator
'''
import types
import sys
import os

from etl.util.ip_convert import IP_Util
from etl.util.playerutil import getplayerInfo
from etl.conf.settings import FlatConfig as Config
from etl.conf.settings import LOGGER

class FlatData:
    def __init__(self,input_path,out_putpath):
        self.playerInfo = None
        self.time_range = None
        self.time_playerinfo = None
        self.input_path = input_path
        self.output_path = out_putpath
        self.flat_buffer = []
        self.AD_SPLIT = Config["ad_split"]
        self.ADLIST_SPLIT = Config["adlist_split"]
        self.batch_write_size = Config["batch_write_size"]
        self.header = Config["header"]
        self.flat_header = Config["flat_header"]
        self.slotid_index = self.header.index(Config["slotid"])
        self.mediabuyid_index = self.header.index(Config["mediabuyid"])
        self.creativeid_index = self.header.index(Config["creativeid"])
        self.ip_index = self.header.index(Config["ip"])
        self.server_timestamp_index = self.header.index(Config["server_timestamp"])
        self.adlist_index = self.header.index(Config['ad_list'])
        self.ip_util=IP_Util(ipb_filepath=Config['ipb_filepath'],
                    city_filepath=Config['city_filepath'])
    
        
    def flat(self):
        self.playerInfo = getplayerInfo()
        self.time_range = get_time_range(self.playerInfo)
        self.time_playerinfo = get_time_playerinfo(self.playerInfo)
        if os.path.exists(self.output_path):
            os.remove(self.output_path)
        first_row = Config["with_header"]
        self.flat_buffer.append(self.flat_header)
        with open(self.input_path,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                if first_row:
                    first_row = False
                    continue
                row = [i.strip() for i in line.strip().split(Config["file_split"])]
                self.flat_in_buffer(row)
        self.write_buffer_in_file()
        self.flat_buffer = []
                
    def flat_in_buffer(self,row):
        
        adlist = row[self.adlist_index]
        try:
            ad_list = self.unpack_adlist(adlist)
            self.flat_buffer = self.pack_list(self.flat_buffer, row, ad_list)
        except Exception,e:
            LOGGER.error("flat adlist error,行:%s error message: %s" % (row,e.message))
            
        if len(self.flat_buffer) >= self.batch_write_size:
            self.write_buffer_in_file()
            self.flat_buffer = []
        
    def write_buffer_in_file(self):
        with open(self.output_path,'a') as fr:
            for row in self.flat_buffer:
                line = get_line(row)
                fr.write(line)
            
    def pack_list(self,new_list,old_row,ad_list):
        '''
                    根据ad_list，组装新的数据list
        '''
        if not new_list:
            new_list=[]
        if not old_row or not len(old_row):
            return new_list
        
        city_row = get_list(old_row)
        self.generate_area_info(city_row)
        if len(ad_list) == 0:
            city_row.append("") #seq
            city_row.append("") #groupid
            new_list.append(city_row)
            return new_list
        seq = 1
        current_groupid = ""
        for i in ad_list:
            if not i or len(i) != 3:
                continue
            new_row = get_list(city_row)
            new_row[self.slotid_index]=i[0]
            new_row[self.mediabuyid_index]=i[1]
            new_row[self.creativeid_index]=i[2]  # TODO
            groupid = self.getgroupid(i[0],new_row[self.server_timestamp_index])
            if groupid != current_groupid:
                seq = 1
            current_groupid = groupid
            new_row.append(seq)
            new_row.append(groupid)
            seq += 1
            new_list.append(new_row)
        return new_list
        
    def getgroupid(self,slotid,s_timestamp):
        time_k = self.get_time_seq(s_timestamp)
        groupid = self.time_playerinfo.get(time_k).get(int(slotid))
        return str(groupid)
        
    def generate_area_info(self,row):
        ip = row[self.ip_index]
        province = self.ip_util.get_cityInfo_from_ip(ip, 2)
        city = self.ip_util.get_cityInfo_from_ip(ip, 4)
        row.append(province)
        row.append(city)
        
    def unpack_adlist(self,adlist):
        ''' adlist = 90,356,432|91,356,435 '''
        if not adlist or not adlist.strip():
            return []
        ad_list = [[i.strip() for i in ad.strip().split(self.AD_SPLIT)] for ad in adlist.strip().split(self.ADLIST_SPLIT)]
        return ad_list
    
    def get_time_seq(self,s_timestamp):
        i_stmp = int(s_timestamp)
        maxk = 0
        for k,v in self.time_range.items():
            if k > maxk:
                maxk = k
            if v[0] <=i_stmp and v[1] >= i_stmp:
                return k
        return maxk
def get_list(row_list):
    if not row_list or not len(row_list):
        return row_list
    
    if type(row_list) is types.TupleType:
        return list(row_list)
    return row_list[:]

def get_line(row):
#     for data in row:
#         line += str(data)+Config["output_column_sep"]
#     line = line.strip(Config["output_column_sep"])
    line = Config["output_column_sep"].join(i for i in row)
    line += '\n'
    return line
    

    
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
        
        
def flat_data(input_path,out_putpath):
    try:
        fd = FlatData(input_path,out_putpath)
        LOGGER.info("flat log ...")
        fd.flat()
    except Exception,e:
        LOGGER.error("flat log file error,filepath:%s . error message: %s"%(input_path,e.message))
        import traceback
        ex=traceback.format_exc()
        LOGGER.error(ex)
        sys.exit(-1)
        
if __name__ == "__main__":
    inputf = "C:/Users/Administrator/Desktop/flat_test/flat_test.csv"
    output = "C:/Users/Administrator/Desktop/flat_test/flat_test_end.csv"
    flat_data(inputf,output)
