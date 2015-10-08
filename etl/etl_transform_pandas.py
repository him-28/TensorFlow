import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
class Etl_Transform_Pandas:
	def __init__(self,filePath,sep,names,groupItem):
		self.df=pd.read_csv(filePath,sep=sep,names=names,header=None) 
		self.grouped = self.df.groupby(groupItem).count()[names[0]]
		self.grouped.to_csv('C:\\test_e.csv',sep=',')
		'''
		file = open('C:\\test_e.csv','w')
		for i in self.grouped.index:
			v = self.grouped[i]
			file.write(v.tostring())
		
		'''
if __name__ == "__main__":
	filePath = r'C:\Users\Administrator\Desktop\20150920.04.product.supply.csv'
	sep = '\t'
	names = ['boardid','deviceid','videoid','slotid','cardid','creativeid','p_v_hid','p_v_rid','p_v_rname','p_c_type','p_c_ip','cityid','intime','p_c_idfa1','p_c_imei','p_c_ctmid','p_c_mac','p_c_anid','p_c_openudid','p_c_idfa','p_c_odin','p_c_aaid','p_c_duid','sid']
	groupItem = ['slotid','cardid','creativeid']
	etp = Etl_Transform_Pandas(filePath,sep,names,groupItem)
	print type(etp.grouped)
	
	
	
	
	
	'''
	
filePath = r'C:\Users\Administrator\Desktop\20150920.04.product.supply.csv'
sep = '\t'
names = ['boardid','deviceid','videoid','slotid','cardid','creativeid','p_v_hid','p_v_rid','p_v_rname','p_c_type','p_c_ip','cityid','intime','p_c_idfa1','p_c_imei','p_c_ctmid','p_c_mac','p_c_anid','p_c_openudid','p_c_idfa','p_c_odin','p_c_aaid','p_c_duid','sid']


groupItem = ['slotid','cardid','creativeid']


df=pd.read_csv(filePath,sep=sep,names=names,header=None)


grouped = df.groupby(groupItem).count()[names[0]]


'''