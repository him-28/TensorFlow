#!/bin/bash
year=$1
month=$2
day=$3
tp=$4
cd /home/dingzheng/inventory/etl/
if [ "$tp" == "pv1" ];then
    python pv1_app.py d
fi
if [ "$tp" == "pv2" ];then
    python pv2_app.py d
fi
if [ "$tp" == "sale" ];then
    python sale_app.py d
fi
if [ $? -eq 255 ];then
    echo "ad inventory day run error"
    #sh send_inventory_mail.sh ${year}${month}${day} ad inventory day run error
    exit
fi
#sh calculate/send_pdf_mail.sh ${year} ${month} ${day}
