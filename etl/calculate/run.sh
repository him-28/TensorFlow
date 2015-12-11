#!/bin/bash

year=$1
month=$2
day=$3
hour=$4

src_path="/home/dingzheng/amble/etl/calculate"
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

if [ "$hour" == "02" ];then
    day2=`date -d '-1 day' +%d`
    nohup sh new_run_day.sh "${year}" "${month}" "${day2}" >> /home/dingzheng/log/etl/ad_day_run.log 2>&1 &
fi

nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "00" >> /home/dingzheng/log/etl/ad_hour_run_2_00.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "15" >> /home/dingzheng/log/etl/ad_hour_run_2_15.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "30" >> /home/dingzheng/log/etl/ad_hour_run_2_30.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "45" >> /home/dingzheng/log/etl/ad_hour_run_2_45.log 2>&1 &

nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "00" >> /home/dingzheng/log/etl/ad_hour_run_3_00.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "15" >> /home/dingzheng/log/etl/ad_hour_run_3_15.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "30" >> /home/dingzheng/log/etl/ad_hour_run_3_30.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "45" >> /home/dingzheng/log/etl/ad_hour_run_3_45.log 2>&1 &

nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "00" >> /home/dingzheng/log/etl/ad_hour_run_4_00.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "15" >> /home/dingzheng/log/etl/ad_hour_run_4_15.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "30" >> /home/dingzheng/log/etl/ad_hour_run_4_30.log 2>&1 &
nohup pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "45" >> /home/dingzheng/log/etl/ad_hour_run_4_45.log 2>&1 &

