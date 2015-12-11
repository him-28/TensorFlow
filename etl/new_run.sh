#!/bin/bash
src_path="/home/dingzheng/amble/etl"

year=$1
month=$2
day=$3
hour=$4

cd ${src_path}

if [ "$year" == "" ];then
    year=`date -d '-1 hour' +%Y`
fi

if [ "$month" == "" ];then
    month=`date -d '-1 hour' +%m`
fi

if [ "$day" == "" ];then
    day=`date -d '-1 hour' +%d`
fi

if [ "$hour" == "" ];then
    hour=`date -d '-1 hour' +%H`
fi

if [ "$hour" == "20" ];then
    day2=`date -d '-1 day' +%d`
    nohup sh ./new_run_day.sh ${year} ${month} ${day2} >> /home/dingzheng/log/etl/ad_day_run.log 2>&1 &
fi


#python new_app.py h

#if [ $? -eq 255 ];then
#    echo "ad inventory hour run error"
#    #sh send_mail.sh ${year}${month}${day} ${hour} ad inventory hour run error
#    exit
#fi
