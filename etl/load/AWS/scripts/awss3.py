

import boto3


class AwsS3:
	default_bucket_name='cn-north-region-java'
	def __init__(self):
		self.s3 = boto3.resource('s3')
		self.bucketsCached = False
		self.bucketsCache = []
	def list_buckets(self,useCache):
		if useCache and self.bucketsCached:
			return self.bucketsCache
		else:
			self.bucketsCached = True
			self.bucketsCache = self.s3.buckets.all()
			return self.bucketsCache
	def list_objects(self,bucketName):
		buckets = self.list_buckets(True)
		bucketNames = []
		isCacheValue = self.bucketsCached
		for b in buckets:
			bucketNames.append(b.name)
		if bucketName not in bucketNames and isCacheValue:
			buckets = self.list_buckets(False)
		
		for bucket in buckets:
			if(bucket.name == bucketName):
				return bucket.objects.all()
		
		return None
	def list_object_key(self,bucketName):
		objects = self.list_objects(bucketName)
		keys = []
		if objects != None:
			for obj in objects:
				keys.append(obj.key)
			return keys
		else:
			return None
	def delete_object(self,bucketName,key):
		objects = self.list_objects(bucketName)
		if objects != None:
			for obj in objects:
				if obj.key == key:
					obj.delete()
	def upload(self,bucketName,key,filePath):
		file = open(filePath,'rb')
		result = self.s3.Bucket(bucketName).put_object(Key=key,Body=file)
		return result
	def download(self,bucketName,key,filePath):
		file = open(filePath,'wb')
		objects = self.list_objects(bucketName)
		if objects != None:
			for obj in objects:
				if obj.key == key:
					fileStreaming = obj.get()['Body']
					f=fileStreaming.read()
					file.write(f)
					
a=AwsS3()
a.list_object_key(AwsS3.default_bucket_name)
a.download(AwsS3.default_bucket_name,'script/test.jar','/home/hadoop/test.csv')