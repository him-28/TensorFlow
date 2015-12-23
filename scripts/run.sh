#!/bin/bash
cnt=$1
pro_path=$(dirname $(readlink -f $0))"/.."
start_t=`date "+%Y-%m-%d %H:%M:%S.%N"`
log_path=$pro_path"/log/run.log"
echo "$start_t start"
cd $pro_path"/etl/"
for (( i=1;i <= $cnt;i++));do
cmd="nohup python new_platform_app.py h $i $cnt >>$log_path 2>&1 &"
echo $cmd | bash
#nohup python new_platform_app.py h $i $cnt 
done
end_t=`date "+%Y-%m-%d %H:%M:%S.%N"`
echo "$end_t end"
