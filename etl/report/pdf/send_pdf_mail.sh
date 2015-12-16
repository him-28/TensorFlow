#!/bin/bash
src_path="/data2/amble/etl"
export LANG=en_US.UTF-8

year=$1
month=$2
day=$3

if [ "$year" == "" ];then
    year=`date -d '-1 day' +%Y`
fi

if [ "$month" == "" ];then
    month=`date -d '-1 day' +%m`
fi

if [ "$day" == "" ];then
    day=`date -d '-1 day' +%d`
fi

pdf_file_path="/data2/ad/${year}/${month}/ad_report_${year}${month}${day}.pdf"

if [ ! -f ${pdf_file_path} ];then
    echo "ad report pdf file not exists"
    sh ${src_path}/send_mail.sh ad_report_${year}${month}${day}.pdf  ad report pdf file  not exists
    #sh ${src_path}/send_mail.sh  ad_report_${year}${month}${day}.pdf  ad report pdf file  not exists 测试邮件，请忽略
    exit
fi

mutt -s "广告数据日报" weixiong@mgtv.com,martin@e.hunantv.com,jinyibin@e.hunantv.com -a ${pdf_file_path} < ./report/pdf/mail_text.txt