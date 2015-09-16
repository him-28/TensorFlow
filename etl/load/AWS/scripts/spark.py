'''/usr/bin/pyspark'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *

conf = SparkConf().setMaster("local[*]")
conf = conf.setAppName("MyAPP")
sc   = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
lines = sc.textFile("s3://cn-north-region-java/data/product.supply1.csv")
parts = lines.map(lambda l: l.split())
product = parts.map(lambda p: (p[0],p[1],p[2],p[3],p[4],p[5],p[6],p[7],p[8],p[9],p[10],p[11],p[12],p[13],p[14],p[15],p[16],p[17],p[18],p[19],p[20],p[21],p[22]))

schemaString = "boardid\tdeviceid\tvoideoid\tslotid\tcardid\tcreativeid\tp_v_hid\tp_v_rid\tp_v_rname\tp_c_type\tp_c_ip\tcityid\tintime\tp_c_idfa\tp_c_imei\tp_c_ctmid\tp_c_mac\tp_c_anid\tp_c_openudid\tp_c_idfa\tp_c_odin\tp_c_aaid\tp_c_duid"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

productPeople = sqlContext.createDataFrame(product, schema)
productPeople.registerTempTable("PRODUCT_SUPPLY")
results = sqlContext.sql("SELECT SUM(voideoid) total, p_c_type FROM PRODUCT_SUPPLY GROUP BY p_c_type ")

names = results.map(lambda p: "ip: " + p.p_c_type + "," + "total: " + str(p.total))
for name in names.collect():
  print(name)