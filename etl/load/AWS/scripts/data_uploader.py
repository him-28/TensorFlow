from awss3 import AwsS3
import datetime
import os
import os.path

# demand_root_path = 'C:\\home\\wn\\amble\\etl\\load\\demand\\' #windows
demand_root_path = '/home/wn/amble/etl/load/demand/' #linux

s3 = AwsS3()
def get_double_number(n):
	if n < 10:
		return '0' + str(n)
	else:
		return str(n)
def load_yestoday():
	today = datetime.date.today()
	yestody = today - datetime.timedelta(days=1)
	yestodyMonthStr = str(yestody.year)+get_double_number(yestody.month)
	yestodyDateStr = yestodyMonthStr+get_double_number(yestody.day)
	for parent,dirnames,filenames in os.walk(demand_root_path): #1.父目录 2.所有文件夹名字（不含路径） 3.所有文件名字
		for dirname in  dirnames:
			if yestodyMonthStr == dirname: # 匹配到月份
				id = parent.replace(demand_root_path,'')
				for c_parent,c_dirnames,c_filenames in os.walk(os.path.join(parent,dirname)):
					for filename in c_filenames:
						if(filename.startswith(yestodyDateStr)):
							print c_parent
							print filename
							print 'upload:' + os.path.join(c_parent,filename)
							s3.upload(AwsS3.default_bucket_name,id+'/'+yestodyMonthStr+'/'+yestodyDateStr+'/'+filename,os.path.join(c_parent,filename))