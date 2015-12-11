#!/bin/bash
year=$1
month=$2
day=$3

python new_app.py d
if [ $? -eq 255 ];then
    echo "ad inventory day run error"
    #sh send_mail.sh ${year}${month}${day} ad inventory day run error
    exit
fi
sh calculate/send_pdf_mail.sh ${year} ${month} ${day}
