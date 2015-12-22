#!/bin/bash

year=$1
month=$2
day=$3
hour=$4

src_path="/home/dingzheng/inventory/etl/calculate"
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

echo "${year},${month},${day},${hour}"

if [ "$hour" == "02" ];then
    day2=`date -d '-1 day' +%d`
    nohup sh /home/dingzheng/inventory/etl/calculate/new_run_day.sh "${year}" "${month}" "${day2}" >> /home/dingzheng/log/etl/ad_day_run.log 2>&1 &
fi

nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "00" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data2 00 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "15" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data2 15 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "30" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data2 30 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "45" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data2 45 >> /dev/null 2>&1 &"

nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "00" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data3 00 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "15" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data3 15 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "30" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data3 30 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "45" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data3 45 >> /dev/null 2>&1 &"

nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "00" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data4 00 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "15" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data4 15 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "30" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data4 30 >> /dev/null 2>&1 &"
nohup sh time_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "45" >> /dev/null 2>&1 &
echo "nohup sh time_pretty_dash.sh ${year} ${month} ${day} ${hour} data4 45 >> /dev/null 2>&1 &"
