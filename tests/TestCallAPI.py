import httplib
import urllib
import urllib2

reqParam = {'
	dateRange:{
		start:"2015-09-16",
		end:"2015-09-17"
	},
	ids:["1","2"],
	name:"¶©µ¥Ãû³Æ",
	priDim:{date:"day"},
	subDim:{ad:"mediabuy"}
'}
reqParamUrlencode = urllib.urlencode(reqParam)
requrl = "http://hostname/amp/orderReport/"
req = urllib2.Request(url=requrl,data=reqParamUrlencode)
print req
resData = urllib2.urlopen(req)
res = res_data.read()
print res