#opyright (c) 2013 hunantv.com, Inc. All Rights Reserved
## $Id: send_mail.sh,v 0.0 Thu 09 Apr 2015 10:01:46 AM CST  dongjie Exp $
##
############################################################################
#
###
# # @file   send_mail.sh
# # @author dongjie<dongjie@e.hunantv.com>
# # @date   Thu 09 Apr 2015 10:01:46 AM CST
# # @brief
# #
# ##
cont=$*

#send mail
URL="http://10.100.4.245/ses/sendEmail"
USER="bigdata"
PWD="bigdata^@hunantv"
SES_appid="bigdata"
SES_pwd="CD435D4F1620BECCA029CBEFA99209AA"
ip_addr=$(ifconfig|grep "inet addr:"|grep -v "127.0.0.1"|cut -d: -f2|awk '{print $1}')
SES_content="${cont}</hr>@${ip_addr}"
SES_address="martin@e.hunantv.com,jinyibin@e.hunantv.com,dingzheng@mgtv.com" 
SES_title="adlog_etl_error"
SES_fromName="ad_log"
SES_fromAddress="dongjie@e.hunantv.com"
SES_sendType="rightnow"

for address in ${SES_address[@]}
do
  POST="SES_appid="$SES_appid"&SES_pwd="$SES_pwd"&SES_content="$SES_content"&SES_address="$address"&SES_title="$SES_title"&SES_fromName="$SES_fromName"&SES_fromAddress="$SES_fromAddress
  curl -d "$POST" $URL
done
## vim: set ts=2 sw=2: #
