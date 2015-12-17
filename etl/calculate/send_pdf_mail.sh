#!/bin/bash
export LANG=en_US.UTF-8
src_path="/data2/amble/etl"

year=$1
month=$2
day=$3

pdf_file_path="/data6/inventory/${year}/${month}/inventory_report_${year}${month}${day}.pdf"

if [ ! -f ${pdf_file_path} ];then
    echo "ad report pdf file not exists: ${pdf_file_path}"
    sh ${src_path}/send_mail.sh inventory_report_${year}${month}${day}.pdf ad report pdf file not exists
    exit
fi

mutt -s "广告库存数据统计日报" martin@e.hunantv.com,jinyibin@e.hunantv.com,dingzheng@mgtv.com,scientsong@e.hunantv.com,dandanhjj@e.hunantv.com,wangqiang@e.hunantv.com -a ${pdf_file_path} < ./calculate/mail_text.txt
