#-*- coding: utf-8 -*-
'''
Created on 2015年9月10日

@author: jyb
'''
import socket,struct
import init_log
LOGGER = init_log.init("logger.conf", 'petlLogger')

class IP_Util:
    ipb_arr=[]
    cityid_arr={}
    def __init__(self,ipb_filepath=None,city_filepath=None):
        self.load_ipb_file(ipb_filepath)
        self.load_city_province_file(city_filepath)
        
    def convert_value_to_ip(self,long_value):
        ''' convert 192.168.1.1 to 3232235777 '''
        try:
            ip=socket.inet_ntoa(struct.pack("!L",long(long_value))) if long_value else None
            return ip
        except Exception,e:
            LOGGER.error("ERROR:convert_ip error:"+e.message)
        return None
    
    def convert_ip_to_value(self,ip):
        ''' convert 3232235777 to 192.168.1.1 '''
        try:
            int_s = socket.ntohl(struct.unpack("L",socket.inet_aton(str(ip)))[0])
            return int_s
        except Exception,e:
            LOGGER.error("ERROR:convert_ip error:"+e.message)
        return None
     
    def load_ipb_file(self,file_path):
        ''' load ipb file into ipb_arr[] '''
        with open(file_path,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                ip_dic=self.ipb_parse_row(line)
                if ip_dic:
                    self.ipb_arr.append(ip_dic)
        return self.ipb_arr
    
    def load_city_province_file(self,file_path):
        ''' load city_province_id into cityid_arr{} '''
        with open(file_path,'rb') as fr:
            for line in fr:
                if not line or not line.strip():
                    continue
                ip_dic=self.city_province_parse_row(line)
                if ip_dic:
                    self.cityid_arr.update(ip_dic)
        return self.cityid_arr
    
    def ipb_parse_row(self,row):
        if not row:
            return None
        try:
            ipb_a=[i.strip() for i in row.strip().split(',')]
            if len(ipb_a) != 4: 
                return None
            ip_start=self.convert_ip_to_value(ipb_a[0])
            ip_end=self.convert_ip_to_value(ipb_a[1])
            return [[ip_start,ip_end],ipb_a[2]]
        except Exception,e:
            LOGGER.error("parse ipb file error"+e.message+" "+row)
    
    def city_province_parse_row(self,row):
        if not row:
            return None
        try:
            city_p_a=[i.strip() for i in row.strip().split(',')]
            if len(city_p_a) != 5:
                return None
            return {city_p_a[4]:[city_p_a[0],city_p_a[1],city_p_a[2],city_p_a[3]]}
        except Exception,e:
            LOGGER.error("parse city_province file error"+e.message+" "+row)
            
    def get_cityInfo_from_value(self,ip_long_value,vtype):
        ''' 根据类型vtype获取ip值的相关信息，省份，城市 '''
        infoid=self.get_ip_infoid(ip_long_value)    
        if not self.cityid_arr.has_key(infoid):
            return None
        if vtype == 1:
            return self.cityid_arr[infoid][0]
        elif vtype == 2:
            return self.cityid_arr[infoid][1]
        elif vtype == 3:
            return self.cityid_arr[infoid][2]
        elif vtype == 4:
            return self.cityid_arr[infoid][3]
        
    
    def get_cityInfo_from_ip(self,ip,vtype):
        long_value=self.convert_ip_to_value(ip)
        return self.get_cityInfo_from_value(long_value,vtype)
    
    def get_ip_infoid(self,ip_long_value):
        ''' 获取ip的infoId '''
        lenght=len(self.ipb_arr)
        mid=lenght/2
        premid=0
        nextmid=lenght
        return self.binary_search(ip_long_value, mid, premid, nextmid)
    
    def binary_search(self,value,mid,premid,nextmid):
        ''' 二分递归查找ip的infoId '''
        if value>self.ipb_arr[mid][0][1]:
            premid=mid
            mid=(mid+nextmid)/2
            return self.binary_search(value, mid, premid, nextmid)
        elif value < self.ipb_arr[mid][0][0]:
            nextmid=mid
            mid=(premid+mid)/2
            return self.binary_search(value, mid, premid, nextmid)
        elif value <=self.ipb_arr[mid][0][1] and value >= self.ipb_arr[mid][0][0]:
            return self.ipb_arr[mid][1]
        else:
            return None

if __name__ == "__main__":
    ip_util=IP_Util(ipb_filepath="C:\Users\Administrator\Desktop\IPB(1).csv",
                    city_filepath="C:\Users\Administrator\Desktop\city_province_2.csv")
    
    print ip_util.get_cityInfo_from_ip("210.47.5.6",1)
    print ip_util.get_cityInfo_from_ip("210.47.5.6",2)
    print ip_util.get_cityInfo_from_ip("210.47.5.6",3)
    print ip_util.get_cityInfo_from_ip("210.47.5.6",4)
    