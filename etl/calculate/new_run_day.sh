#!/bin/bash
year=$1
month=$2
day=$3
cd /home/dingzheng/inventory/etl/
python new_app.py d
if [ $? -eq 255 ];then
    echo "ad inventory day run error"
    sh send_inventory_mail.sh ${year}${month}${day} ad inventory day run error
    exit
fi
