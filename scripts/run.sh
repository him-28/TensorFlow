#!/bin/bash
cnt=$1
pro_path=$(dirname $(readlink -f $0))"/.."
cmd=$pro_path"/etl/new_platform_app.py"
start_t=`date "+%Y-%m-%d %H:%M:%S.%N"`
echo "$start_t start"

for (( i=1;i <= $cnt;i++));do
nohup python $cmd h $i $cnt >/dev/null 2>&1 &
done
end_t=`date "+%Y-%m-%d %H:%M:%S.%N"`
echo "$end_t start"
