#encoding=utf8
#!/bin/bash

year=`date -d '-1 hour' +%Y`
month=`date -d '-1 hour' +%m`
day=`date -d '-1 hour' +%d`
hour=`date -d '-1 hour' +%H`

prefix2="data2"
prefix3="data3"
prefix4="data4"

dash00="00"
dash15="15"
dash30="30"
dash45="45"

f1="/home/dingzheng/.inventory_${prefix2}_${year}${month}${day}${hour}${dash00}.sale"
f2="/home/dingzheng/.inventory_${prefix2}_${year}${month}${day}${hour}${dash15}.sale"
f3="/home/dingzheng/.inventory_${prefix2}_${year}${month}${day}${hour}${dash30}.sale"
f4="/home/dingzheng/.inventory_${prefix2}_${year}${month}${day}${hour}${dash45}.sale"

f5="/home/dingzheng/.inventory_${prefix3}_${year}${month}${day}${hour}${dash00}.sale"
f6="/home/dingzheng/.inventory_${prefix3}_${year}${month}${day}${hour}${dash15}.sale"
f7="/home/dingzheng/.inventory_${prefix3}_${year}${month}${day}${hour}${dash30}.sale"
f8="/home/dingzheng/.inventory_${prefix3}_${year}${month}${day}${hour}${dash45}.sale"

f9="/home/dingzheng/.inventory_${prefix4}_${year}${month}${day}${hour}${dash00}.sale"
f10="/home/dingzheng/.inventory_${prefix4}_${year}${month}${day}${hour}${dash15}.sale"
f11="/home/dingzheng/.inventory_${prefix4}_${year}${month}${day}${hour}${dash30}.sale"
f12="/home/dingzheng/.inventory_${prefix4}_${year}${month}${day}${hour}${dash45}.sale"

if [ ! -f ${f1} ];then
    exit
fi
echo "find ${f1}"
if [ ! -f ${f2} ];then
    exit
fi
echo "find ${f2}"
if [ ! -f ${f3} ];then
    exit
fi
echo "find ${f3}"
if [ ! -f ${f4} ];then
    exit
fi
echo "find ${f4}"
if [ ! -f ${f5} ];then
    exit
fi
echo "find ${f5}"
if [ ! -f ${f6} ];then
    exit
fi
echo "find ${f6}"
if [ ! -f ${f7} ];then
    exit
fi
echo "find ${f7}"
if [ ! -f ${f8} ];then
    exit
fi
echo "find ${f8}"
if [ ! -f ${f9} ];then
    exit
fi
echo "find ${f9}"
if [ ! -f ${f10} ];then
    exit
fi
echo "find ${f10}"
if [ ! -f ${f11} ];then
    exit
fi
echo "find ${f11}"
if [ ! -f ${f12} ];then
    exit
fi
echo "find ${f12}"

cd /home/dingzheng/amble/etl/

r1="/data6/inventory/${year}/${month}/${day}/inventory_${prefix2}_${year}${month}${day}${hour}${dash00}.sale"
r2="/data6/inventory/${year}/${month}/${day}/inventory_${prefix2}_${year}${month}${day}${hour}${dash15}.sale"
r3="/data6/inventory/${year}/${month}/${day}/inventory_${prefix2}_${year}${month}${day}${hour}${dash30}.sale"
r4="/data6/inventory/${year}/${month}/${day}/inventory_${prefix2}_${year}${month}${day}${hour}${dash45}.sale"
r5="/data6/inventory/${year}/${month}/${day}/inventory_${prefix3}_${year}${month}${day}${hour}${dash00}.sale"
r6="/data6/inventory/${year}/${month}/${day}/inventory_${prefix3}_${year}${month}${day}${hour}${dash15}.sale"
r7="/data6/inventory/${year}/${month}/${day}/inventory_${prefix3}_${year}${month}${day}${hour}${dash30}.sale"
r8="/data6/inventory/${year}/${month}/${day}/inventory_${prefix3}_${year}${month}${day}${hour}${dash45}.sale"
r9="/data6/inventory/${year}/${month}/${day}/inventory_${prefix4}_${year}${month}${day}${hour}${dash00}.sale"
r10="/data6/inventory/${year}/${month}/${day}/inventory_${prefix4}_${year}${month}${day}${hour}${dash15}.sale"
r11="/data6/inventory/${year}/${month}/${day}/inventory_${prefix4}_${year}${month}${day}${hour}${dash30}.sale"
r12="/data6/inventory/${year}/${month}/${day}/inventory_${prefix4}_${year}${month}${day}${hour}${dash45}.sale"

echo "python merge_crontab.py ${f1},${f2},${f3},${f4},${f5},${f6},${f7},${f8},${f9},${f10},${f11},${f12}  ${r1},${r2},${r3},${r4},${r5},${r6},${r7},${r8},${r9},${r10},${r11},${r12}  /data6/inventory/${year}/${month}/${day}/inventory_sale_${hour}.csv display_sale"

python merge_crontab.py ${f1},${f2},${f3},${f4},${f5},${f6},${f7},${f8},${f9},${f10},${f11},${f12}  ${r1},${r2},${r3},${r4},${r5},${r6},${r7},${r8},${r9},${r10},${r11},${r12}  /data6/inventory/${year}/${month}/${day}/inventory_sale_${hour}.csv display_sale