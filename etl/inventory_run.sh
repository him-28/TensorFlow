#!/bin/bash
#yyyy-MM-DD:HH
date=$1
hour=$2
echo "python inventory_app.py inventory h ${date}"
python inventory_app.py inventory h "${date}"
if [ $? -eq 255 ];then
    echo "ad inventory hour run error"
    sh send_mail.sh ${date} ad inventory hour run error
    exit
fi
if [ "${hour}" == "03" ];then
    python inventory_app.py inventory 'd'
    if [ $? -eq 255 ];then
        echo "ad inventory day run error"
        sh send_mail.sh ${date} ad inventory day run error
        exit
    fi
fi