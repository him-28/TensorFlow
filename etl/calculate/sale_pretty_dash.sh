#!/bin/bash

year=$1
month=$2
day=$3
hour=$4
prefix=$5
dash=$6

cd ../
python sale_app.py h /${prefix}/ngx/${year}/${month}/${day}/log.da.hunantv.com-access.log-${year}${month}${day}${hour}${dash} /data6/inventory/${year}/${month}/${day}/inventory_${prefix}_${year}${month}${day}${hour}${dash} /home/dingzheng/.inventory_${prefix}_${year}${month}${day}${hour}${dash}.sale
echo "python sale_app.py h /${prefix}/ngx/${year}/${month}/${day}/log.da.hunantv.com-access.log-${year}${month}${day}${hour}${dash} /data6/inventory/${year}/${month}/${day}/inventory_${prefix}_${year}${month}${day}${hour}${dash} /home/dingzheng/.inventory_${prefix}_${year}${month}${day}${hour}${dash}.sale"
if [ $? -eq 255 ];then
    echo "ad inventory hour run error"
    sh send_inventory_mail.sh ${year}${month}${day} ${hour} ad inventory display_sale hour run error
fi
cd calculate
