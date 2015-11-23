#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: jyb
'''
import petl as etl
import datetime
import sys
import os
import types

from etl.conf.settings import MONITOR_CONFIGS as Config
from etl.conf.settings import ptLogger as LOGGER
from etl.util.playerutil import getplayerInfo 

REAL_SLOTID = Config["addslotid"]
class ETL_Transform:
    ADLIST_SPLIT="|"
    AD_SPLIT=","
    def __init__(self,header,
                 filePath,output_dic,batch_read_size=5000):
        self.header=header
        self.filePath=filePath
        self.batch_read_size=batch_read_size
        self.chance_select=Config["chance_select"]
        self.count_select=Config["count_select"]
        self.seq_select = Config["seq_select"]
        self.up_select = Config["up_select"]
        self.start_select=Config["start_select"]
        self.end_select=Config["end_select"]
        self.click_select=Config["click_select"]
        self.chance_aggre_header=Config["chance_aggre_header"]
        self.count_aggre_header=Config["count_aggre_header"]
        self.seq_aggre_header = Config["seq_aggre_header"]
        self.up_aggre_header = Config["up_aggre_header"]
        self.start_aggre_header=Config["start_aggre_header"]
        self.end_aggre_header=Config["end_aggre_header"]
        self.click_aggre_header=Config["click_aggre_header"]
        self.chance_merge_table=[]
        self.count_merge_table=[]
        self.seq_merge_table = []
        self.up_merge_table = []
        self.start_merge_table=[]
        self.end_merge_table=[]
        self.click_merge_table=[]
        self.chance_agg_header_=self.chance_aggre_header[:]
        self.chance_agg_header_.append('value')
        self.count_agg_header_=self.count_aggre_header[:]
        self.count_agg_header_.append('value')
        self.seq_agg_header_=self.seq_aggre_header[:]
        self.seq_agg_header_.append('value')
        self.up_agg_header_=self.up_aggre_header[:]
        self.up_agg_header_.append('value')
        self.start_agg_header_=self.start_aggre_header[:]
        self.start_agg_header_.append('value')
        self.end_agg_header_=self.end_aggre_header[:]
        self.end_agg_header_.append('value')
        self.click_agg_header_=self.click_aggre_header[:]
        self.click_agg_header_.append('value')
        self.chance_merge_table.append(self.chance_agg_header_)
        self.count_merge_table.append(self.count_agg_header_)
        self.seq_merge_table.append(self.seq_agg_header_)
        self.up_merge_table.append(self.up_agg_header_)
        self.start_merge_table.append(self.start_agg_header_)
        self.end_merge_table.append(self.end_agg_header_)
        self.click_merge_table.append(self.click_agg_header_)
        self.read_buffer=[]
        self.chance_buffer=[]
        self.count_buffer=[]
        self.seq_buffer = []
        self.up_buffer = []
        self.imps_start_buffer=[]
        self.imps_end_buffer=[]
        self.click_buffer=[]
        self.output_dic=output_dic
        self.playerinfo={}
        self.newestPlayerInfo = {}
        self.newestPlayerSlotInfo = {}
        
    def transform_start(self):
        LOGGER.info("etl...")
        self.read_buffer=[]
        
        self.playerinfo=getplayerInfo()
        self.newestPlayerInfo = self.getnewestPlayerInfo()
        self.newestPlayerSlotInfo = self.getnewestPlayerSlotInfo()
        # 目前暂取最大区间处理
        
        #TODO 转换成{4820：{group11201:{seq1:slotid1,seq2:slotid2},group11202:{seq1:slota1,seq2:slota2}}}
#         self.playerinfo={'4820':{'110':[{'130':"正一"},{"131":"正二"}],"111":[{"132":"中贴一"}]}}
        tag_index = self.header.index(Config["tag"])
        with_header = Config["with_header"]
        first_row = True
        with open(self.filePath,'rb') as fr:
            for line in fr:
                if not line:
                    continue
                row=[i.strip() for i in line.split(Config["input_column_sep"])]
                if first_row and with_header:
                    self.header = row
                    first_row = False
                    continue
                row[tag_index] = int(row[tag_index])
#                 print row
                self.read_in_buffer(row)
                
        print "read over ..."
        self.read_buffer_in_table()
        self.read_buffer=[]
        self.chance_merge_table = etl.rename(self.chance_merge_table,'value','total')
        self.count_merge_table = etl.rename(self.count_merge_table,'value','total')
        self.start_merge_table = etl.rename(self.start_merge_table,'value','total')
        self.end_merge_table = etl.rename(self.end_merge_table,'value','total')
        self.click_merge_table = etl.rename(self.click_merge_table,'value','total')
        self.seq_merge_table = etl.rename(self.seq_merge_table,'value','total')
        self.up_merge_table = etl.rename(self.up_merge_table,{REAL_SLOTID:'slot_id','value':'total'})
        
    def getnewestPlayerInfo(self):    
        maxk = 0
        for k,v in self.playerinfo.items():
            if k > maxk:
                maxk = k
        newest_player_info = {}
        maxplayer_info = self.playerinfo.get(maxk).get("playerinfo")
        #TODO 转换成{4820：{group11201:{seq1:slotid1,seq2:slotid2},group11202:{seq1:slota1,seq2:slota2}}}
        for k,v in maxplayer_info.items():
            _player_info = {}
            newest_player_info[str(k)] = _player_info
            for ki,vi in v.items():
                groupinfo = _player_info.get(str(vi[0]))
                if not groupinfo:
                    groupinfo = {}
                    _player_info[str(vi[0])] = groupinfo
                groupinfo[str(vi[2])] = str(ki)
                
        return newest_player_info
    
    def getnewestPlayerSlotInfo(self):
        '''广告位不是有序的'''
        maxk = 0
        for k,v in self.playerinfo.items():
            if k > maxk:
                maxk = k
        newest_player_info = {}
        maxplayer_info = self.playerinfo.get(maxk).get("playerinfo")
        for k,v in maxplayer_info.items():
            _player_info = []
            newest_player_info[str(k)] = _player_info
            for ki,vi in v.items():
                _player_info.append(str(ki))
        return newest_player_info    
        
            
    def etl_display_chance(self):
        #展示机会  和播放器下广告位相关
        self.chance_buffer = self.update_chance_buffer(self.chance_buffer)
        self.chance_merge_table = self.etl_aggregate(self.chance_buffer,self.chance_aggre_header,self.chance_merge_table)
    def etl_display_count(self):
        #升位数  广告次序展示数，  售卖广告展示数（广告位id展示数）
#         self.count_buffer = self.update_count_buffer(self.count_buffer)
        self.count_merge_table = self.etl_aggregate(self.count_buffer,self.count_aggre_header,self.count_merge_table)
        
    def etl_seq_display_count(self):
        self.seq_merge_table = self.etl_aggregate(self.seq_buffer,self.seq_aggre_header,self.seq_merge_table)
    def etl_up_count(self):
        self.up_buffer = self.update_up_buffer(self.up_buffer)
        _up_header = self.up_aggre_header[:]
#         _up_header.append(REAL_SLOTID)
        self.up_merge_table = self.etl_aggregate(self.up_buffer,_up_header,self.up_merge_table)
    def etl_imps_start(self):
        self.start_merge_table = self.etl_aggregate(self.imps_start_buffer,self.start_aggre_header,self.start_merge_table)
    def etl_imps_end(self):
        self.end_merge_table = self.etl_aggregate(self.imps_end_buffer,self.end_aggre_header,self.end_merge_table)
    def etl_click(self):
        self.click_merge_table = self.etl_aggregate(self.click_buffer,self.click_aggre_header,self.click_merge_table)
    
    def update_chance_buffer(self,chance_buffer):
        _new_chance_buffer = []
        _new_chance_buffer.append(self.header)
        first_row = True
        for row in chance_buffer:
            if first_row: 
                first_row = False
                continue
            _new_chance_buffer.append(row)
            new_rows = self.getchance_row(row)
            for new_row in new_rows:
                _new_chance_buffer.append(new_row)
        return _new_chance_buffer 
        
    def getchance_row(self,row):
        seq_index = self.header.index(Config["seq"])
        playerid_index = self.header.index(Config["player_id"])
        adlist_index = self.header.index(Config["ad_list"])
        mediabuyid_index = self.header.index(Config["mediabuyid"])
        creativeid_index = self.header.index(Config["creativeid"])
        slotid_index = self.header.index(Config["slotid"])
        if row[seq_index] and row[seq_index] != '1'and row[seq_index] != '-1':
            return []
        playerid = row[playerid_index]
        playerSlots = self.newestPlayerSlotInfo.get(playerid)
        
        adlist = row[adlist_index]
        adslots = self.unpack_adlist(adlist)
        new_rows = []
        for slot in playerSlots:
            if adslots.count(slot) == 1:
                continue
            new_row = get_list(row)
            new_row[seq_index] = ""
            new_row[mediabuyid_index] = "-1"
            new_row[creativeid_index] = "-1"
            new_row[slotid_index] = slot
            new_rows.append(new_row)
        return new_rows
            
        
    def unpack_adlist(self,adlist):
        ''' adlist = 90,356,432|91,356,435 '''
        if not adlist or not adlist.strip():
            return []
        ad_list = [[i.strip() for i in ad.strip().split(self.AD_SPLIT)][0] for ad in adlist.strip().split(self.ADLIST_SPLIT)]
        return ad_list
        
            
        
    def update_up_buffer(self,up_buffer):
        header_ = self.header[:]
        header_.append(REAL_SLOTID)
        _new_up_buffer = []
        _new_up_buffer.append(header_)
        first_row = True
        for row in up_buffer:
            if first_row: 
                first_row = False
                continue
            try:
                new_row = self.getRealSlotId_by_seq(row)
                if not new_row is None:
                    _new_up_buffer.append(new_row)
            except Exception,e:
                LOGGER.error("get real slot id error,error message:%s" % e.message)
        return _new_up_buffer 
    def getRealSlotId_by_seq(self,row):
        playerid_index = self.header.index(Config["player_id"])
        group_index = self.header.index(Config["group_id"])
        seq_index = self.header.index(Config["seq"])
        slotid_index = self.header.index(Config["slotid"])
        
        playerid = row[playerid_index]
        groupid = row[group_index]
        seq = row[seq_index]
        slotid = row[slotid_index]
        realslotid = ""
        if playerid and groupid and seq and slotid:
            try:
                realslotid = self.newestPlayerInfo[playerid][groupid][seq]
            except Exception,e:
                LOGGER.error("get slot id from playerinfo error,playerid:%s groupid:%s seq:%s ,error message:%s"%(playerid,groupid,seq,e.message))
        else:
            return None
        if not realslotid or not realslotid.strip():
            LOGGER.error("not find slotid in  playerid:%s group_id:%s seq:%s" % (playerid,groupid,seq))
        if slotid != realslotid:    
            _row = get_list(row)
            _row.append(realslotid) 
            return _row 
        else:
            return None
    
    
    def etl_aggregate(self,t_buffer,aggre_header,merge_table):
#         row_table=[]
#         row_table.append(self.header)
#         row_table.extend(t_buffer)
        row_table=t_buffer
        agg_header_ = aggre_header[:]
        
        tuple_key=tuple(aggre_header)
        if len(aggre_header) == 1:
            tuple_key=aggre_header[0]
        display_table = etl.aggregate(row_table,tuple_key,len)
        agg_header_.append('value')
        tmp_merge_table=self.union_table(merge_table, display_table)
#         tmp_2_merge_table=etl.convert(tmp_merge_table,"value",lambda x:int(x))
        table_t = etl.aggregate(tmp_merge_table,tuple_key,sum,'value')
        return table_t
        
    def read_in_buffer(self,row):
        if len(self.read_buffer) == 0:
            self.read_buffer.append(self.header)
        if len(self.read_buffer) <self.batch_read_size:
            self.read_buffer.append(row)
        else:
            self.read_buffer.append(row)
            self.read_buffer_in_table()
            self.read_buffer=[]
    def read_buffer_in_table(self):
#         self.read_buffer = etl.convert(self.read_buffer,Config["tag"],lambda x:int(x))
        self.chance_buffer=self.select_data(self.read_buffer, self.chance_select)
        try:
            self.etl_display_chance()
        except Exception,e:
            LOGGER.error("calcuclate display_poss error,error meesage:%s"% e.message)
        self.chance_buffer=None
        
        self.seq_buffer = self.select_data(self.read_buffer, self.seq_select)
        try:
            self.etl_seq_display_count()
        except Exception,e:
            LOGGER.error("calcuclate display error,error meesage:%s"% e.message)   
        self.seq_buffer = None
        
        self.up_buffer=self.select_data(self.read_buffer, self.up_select)
        try:
            self.etl_up_count()
        except Exception,e:
            LOGGER.error("calcuclate display_up error,error meesage:%s"% e.message)
        self.up_buffer=None
        
        self.count_buffer=self.select_data(self.read_buffer, self.count_select)
        try:
            self.etl_display_count()
        except Exception,e:
            LOGGER.error("calcuclate display_sale error,error meesage:%s"% e.message)
        self.count_buffer=None
        
        self.imps_start_buffer=self.select_data(self.read_buffer, self.start_select)
        try:
            self.etl_imps_start()
        except Exception,e:
            LOGGER.error("calcuclate impression_start error,error meesage:%s"% e.message)
        self.imps_start_buffer=None
        
        self.imps_end_buffer=self.select_data(self.read_buffer, self.end_select)
        try:
            self.etl_imps_end()
        except Exception,e:
            LOGGER.error("calcuclate impression_end error,error meesage:%s"% e.message)
        self.imps_end_buffer=None
        
        self.click_buffer=self.select_data(self.read_buffer, self.click_select)
        try:
            self.etl_click()
        except Exception,e:
            LOGGER.error("calcuclate click error,error meesage:%s"% e.message)
        self.click_buffer=None
        
            
    def select_data(self,rows,q_select):
        tmp_table=[]
        tmp_table.append(self.header)
        tmp_table.extend(rows)
        t_=etl.select(tmp_table,q_select)
        return t_
    def union_table(self,table1,table2):
        try:
            tmp_table = table1.list()
        except:
            tmp_table = table1
        try:
            tmp_table2 = table2.list()
        except:
            tmp_table2 = table2
        tmp_table.extend(tmp_table2[1:])
        return tmp_table
    
    def transform(self):
        self.transform_start()
        
        if not os.path.exists(self.filePath):
            os.makedirs(self.filePath)
            
        chance_file=self.output_dic.get(Config["display_pos"])
        # TODO FIXME 临时修改
        seq_file=self.output_dic.get(Config["display"])
        # TODO FIXME 临时修改
        sale_file=self.output_dic.get(Config["display_sale"])
        up_file = self.output_dic.get(Config["up"])
        start_file=self.output_dic.get(Config["impression"])
        end_file=self.output_dic.get(Config["impression_end"])
        click_file=self.output_dic.get(Config["click"])

        LOGGER.info("generate "+chance_file)
        etl.tocsv(self.chance_merge_table,chance_file,encoding="utf-8",write_header=True,delimiter=Config["output_column_sep"])

        LOGGER.info("generate "+sale_file)
        etl.tocsv(self.count_merge_table,sale_file,encoding="utf-8",write_header=True,delimiter=Config["output_column_sep"])

        LOGGER.info("generate "+seq_file)
        etl.tocsv(self.seq_merge_table,seq_file,encoding="utf-8",write_header=True,delimiter=Config["output_column_sep"])

        LOGGER.info("generate "+up_file)
        etl.tocsv(self.up_merge_table,up_file,encoding="utf-8",write_header=True,delimiter=Config["output_column_sep"])
        
        LOGGER.info("generate "+start_file)
        etl.tocsv(self.start_merge_table,start_file,encoding="utf-8",write_header=True,delimiter=Config["output_column_sep"])
        
        LOGGER.info("generate "+end_file)
        etl.tocsv(self.end_merge_table,end_file,encoding="utf-8",write_header=True,delimiter=Config["output_column_sep"])
        
        LOGGER.info("generate "+click_file)
        etl.tocsv(self.click_merge_table,click_file,encoding="utf-8",write_header=True,delimiter=Config["output_column_sep"])
        
        
def calc_etl(input_filePath,input_filename,output_dic):
    #check  param
    assert input_filePath,input_filename is not None

    filePath = os.path.join(input_filePath,input_filename)
#     filePath=r"C:\Users\Administrator\Desktop\ad_test_xx.csv"
    etls=ETL_Transform(
                       header=Config['header'],
                       batch_read_size=Config["batch_read_size"],
                       filePath=filePath,output_dic=output_dic)
    try:
        LOGGER.info("petl etl start...")
        etls.transform()
    except Exception,e:
        LOGGER.error(" logic0 calculate error,message: %s"%e.message)
        import traceback
        ex=traceback.format_exc()
        LOGGER.error(ex)
        print ex
        sys.exit(-1)
def get_list(row_list):
    if not row_list or not len(row_list):
        return row_list
    
    if type(row_list) is types.TupleType:
        return list(row_list)
    return row_list[:]
if __name__ == "__main__":
    day = sys.argv[1]
    hour = sys.argv[2]
#     hour_etl(day, hour)
    calc_etl("C:\Users\Administrator\Desktop\\" ,"ad(1).csv",
             {'display_pos':'C:\Users\Administrator\Desktop\\201510\chance.csv',
              'display':'C:\Users\Administrator\Desktop\\201510\count.csv',
              'impression':'C:\Users\Administrator\Desktop\\201510\start.csv',
              'impression_end':'C:\Users\Administrator\Desktop\\201510\end.csv',
              'click':'C:\Users\Administrator\Desktop\\201510\click.csv'})
#     adlist = '90,356,432|91,356,435'
#     if not adlist or not adlist.strip():
#         print  []
#     ad_list = [[i.strip() for i in ad.strip().split(',')] for ad in adlist.strip().split('|')]
#     
#     print ad_list

