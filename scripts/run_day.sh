#!/bin/bash
pro_path=$(dirname $(readlink -f $0))"/.."
start_t=`date "+%Y-%m-%d %H:%M:%S.%N"`
log_path=$pro_path"/log/run_day.log"
echo "$start_t start"
cd $pro_path"/etl/"
cmd="nohup python new_platform_app.py d  >>$log_path 2>&1 &"
echo $cmd | bash
end_t=`date "+%Y-%m-%d %H:%M:%S.%N"`
echo "$end_t end"
