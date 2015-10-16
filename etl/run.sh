############################################################################
##
## Copyright (c) 2013 hunantv.com, Inc. All Rights Reserved
## $Id: run.sh,v 0.0 2015年10月14日 星期二 11时02分08秒  dongjie Exp $
##
############################################################################
#
###
# # @file   run.sh
# # @author dongjie<dongjie@e.hunantv.com>
# # @date   2015年10月14日 星期二 11时02分08秒
# # @brief
# #
# ##
#!/bin/bash
src_path="/data1/amble/etl"

year=$1
month=$2
day=$3
hour=$4

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

echo "==>${year}${month}${day}${hour}"

cd $src_path
python adlog_format_audit.py ${year}${month}${day} ${hour}
if [ $? -eq 255 ];then
    echo "ad log audit error"
    sh send_mail.sh ${year}${month}${day} ${hour} ad log audit error
    exit
fi
python etl_transform.py ${year}${month}${day} ${hour} 'hour' 'new'
if [ $? -eq 255 ];then
    echo "etl transform error"
    sh send_mail.sh ${year}${month}${day} ${hour} etl transform error
    exit
fi
python etl_job_hour.py ${year}${month}${day} ${hour} 'new'
if [ $? -eq 255 ];then
    echo "etl job hour error"
    sh send_mail.sh ${year}${month}${day} ${hour} etl job hour error
    exit
fi
if [ "${hour}" == "23" ];then
    python day_etl_transform.py ${year}${month}${day} 'merge' 'new'
    if [ $? -eq 255 ];then
        echo "day etl transform error"
        sh send_mail.sh ${year}${month}${day} day etl transform error
        exit
    fi
    python etl_job_day.py ${year}${month}${day} 'merge' 'new'
    if [ $? -eq 255 ];then
        sh send_mail.sh ${year}${month}${day} etl job day error
        echo "etl job day error"
        exit
    fi
fi
