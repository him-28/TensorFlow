############################################################################
##
#!/bin/bash
src_path="/data2/amble/etl"
fluentd_dir="/data2/ad/track"

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

## move file
ngx_ad_dir="/data3/ngx/${year}/${month}/${day}/"
fluentd_ad_path="${fluentd_dir}/ad_track.${year}${month}${day}${hour}.log"
ngx_ad_path="${ngx_ad_dir}ad_${hour}.log"

echo "==>fluentd log path:${fluentd_ad_path}"
echo "==>nginx log path:${ngx_ad_path}"

mkdir -p ${ngx_ad_dir}

if [ ! -f ${fluentd_ad_path} ];then
    echo "fluentd source file not exists"
    sh send_mail.sh ${year}${month}${day} ${hour} fluentd source file not exists
    exit
fi

mv ${fluentd_ad_path} ${ngx_ad_path}

cd $src_path
python audit_app.py admonitor 'h'
if [ $? -eq 255 ];then
    echo "ad audit hour run error"
    sh send_mail.sh ${year}${month}${day} ${hour} ad audit hour run error
    exit
fi

nohup ./inventory_run.sh "${year}-${month}-${day}:${hour}" > /dev/null 2>&1 &

python app.py admonitor 'h' "${year}-${month}-${day}:${hour}"
if [ $? -eq 255 ];then
    echo "ad app hour run error"
    sh send_mail.sh ${year}${month}${day} ${hour} ad app hour run error
    exit
fi

if [ "${hour}" == "06" ];then
    python app.py admonitor 'd'
    if [ $? -eq 255 ];then
        echo "ad app day run error"
        sh send_mail.sh ${year}${month}${day} ad app day run error
        exit
    fi
fi
