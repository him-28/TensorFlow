

import boto3

''' 上传文件到Amazon S3 '''

class AwsS3:
	default_bucket_name = 'cn-north-region-java'
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
		buckets = self.listBuckets(True)
		bucketNames = []
		isCacheValue = self.bucketsCached
		for b in buckets:
			bucketNames.append(b.name)
		if bucketName not in bucketNames and isCacheValue:
			buckets = self.listBuckets(False)
		
		for bucket in buckets:
			if(bucket.name == bucketName):
				return bucket.objects.all()
		
		return None
	def list_object_key(self,bucketName):
		objects = self.listObject(bucketName)
		keys = []
		if objects != None:
			for obj in objects:
				keys.append(obj.key)
			return keys
		else:
			return None
	def delete_object(self,bucketName,key):
		objects = self.listObject(bucketName)
		if objects != None:
			for obj in objects:
				if obj.key == key:
					obj.delete()
	def upload(self,bucketName,key,filePath):
		file = open(filePath,'rb')
		result = self.s3.Bucket(bucketName).put_object(Key=key,Body=file)
		return result