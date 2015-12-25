#!/bin/bash

year=$1
month=$2
day=$3
hour=$4
prefix=$5
dash=$6

cd ../
python new_app.py h /${prefix}/ngx/${year}/${month}/${day}/log.da.hunantv.com-access.log-${year}${month}${day}${hour}${dash} /data6/inventory/${year}/${month}/${day}/inventory_${prefix}_${year}${month}${day}${hour}${dash} /home/dingzheng/.inventory_${prefix}_${year}${month}${day}${hour}${dash}.pv1,/home/dingzheng/.inventory_${prefix}_${year}${month}${day}${hour}${dash}.pv2,/home/dingzheng/.inventory_${prefix}_${year}${month}${day}${hour}${dash}.sale >> /dev/null 2>&1 &
if [ $? -eq 255 ];then
    echo "ad inventory hour run error"
    sh send_inventory_mail.sh ${year}${month}${day} ${hour} ad inventory hour run error
fi

if [ "$hour" == "02" ];then
    if [ "$dash" == "45" ];then
        day2=`date -d '-1 day' +%d`
        if [ "$prefix" == "data4" ];then
            python pv2_app.py d
            if [ $? -eq 255 ];then
                echo "ad inventory pv2 day error"
                sh send_inventory_mail.sh ${year}${month}${day} ad inventory pv2 day run error
            fi
            python sale_app.py d
            if [ $? -eq 255 ];then
                echo "ad inventory sale day error"
                sh send_inventory_mail.sh ${year}${month}${day} ad inventory sale day run error
            fi
            python pv1_app.py d
            if [ $? -eq 255 ];then
                echo "ad inventory pv1 day error"
                sh send_inventory_mail.sh ${year}${month}${day} ad inventory pv1 day run error
            fi
        fi
    fi
fi

cd calculate