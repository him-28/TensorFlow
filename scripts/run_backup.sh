#!/bin/bash
cnt=$1
now_time=$2
pro_path=$(dirname $(readlink -f $0))"/.."
start_t=`date "+%Y-%m-%d %H:%M:%S.%N"`
log_path=$pro_path"/log/run_backup.log"
echo "$start_t start"
cd $pro_path"/etl/"
for (( i=1;i <= $cnt;i++));do
cmd="nohup python new_platform_backup.py h $i $cnt $now_time >>$log_path 2>&1 &"
echo $cmd | bash
done
end_t=`date "+%Y-%m-%d %H:%M:%S.%N"`
echo "$end_t end"
