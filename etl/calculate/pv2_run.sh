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

nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "00" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data2 00 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "15" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data2 15 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "30" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data2 30 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data2" "45" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data2 45 "pv2" >> /dev/null 2>&1 &"

nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "00" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data3 00 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "15" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data3 15 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "30" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data3 30 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data3" "45" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data3 45 "pv2" >> /dev/null 2>&1 &"

nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "00" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data4 00 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "15" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data4 15 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "30" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data4 30 "pv2" >> /dev/null 2>&1 &"
nohup sh pv2_pretty_dash.sh "${year}" "${month}" "${day}" "${hour}" "data4" "45" "pv2" >> /dev/null 2>&1 &
echo "nohup sh pv2_pretty_dash.sh ${year} ${month} ${day} ${hour} data4 45 "pv2" >> /dev/null 2>&1 &"
