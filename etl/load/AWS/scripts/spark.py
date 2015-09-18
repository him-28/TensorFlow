'''/usr/bin/pyspark'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import tempfile

class SparkHandler:
	conf = SparkConf().setMaster("local[*]")
	conf = conf.setAppName("S3DataHandler")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)
	def __init__(self,hand_date,hand_sql):
		self.hand_date=hand_date
		self.hand_sql=hand_sql
	def hand_demand(self,object_s3_path,output_file):
		lines = sc.textFile(object_s3_path)
		parts = lines.map(lambda l: l.split())
		product = parts.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10]))
		schemaString = "slotid\tcardid\tcreativeid\tdeviceid\ttype\tintime\tip\tboardid\t_sid\tvoideoid\tsecond"
		fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
		schema = StructType(fields)
		productPeople = sqlContext.createDataFrame(product, schema)
		productPeople.registerTempTable("PRODUCT_SUPPLY")
		results = sqlContext.sql("SELECT * FROM PRODUCT_SUPPLY")
		names = results.map(lambda p: "slotid: " + p.slotid + "," + "tcardid: " + tcardid)
		f = open(output_file,'r+')
		for name in names.collect():
		  f.write(name)
		  f.write('\n')
		f.close()