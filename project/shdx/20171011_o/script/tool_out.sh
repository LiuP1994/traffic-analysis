#!/usr/bin/env bash
if [ $# != 1 ]; then
echo "Please enter the target date like 20130901"
exit 1
fi

rundir=/home/e_ruichuang/shyjy/20171011
bigdataJar=${rundir}/analysis-1.0-SNAPSHOT.jar
hive_database=e_ruichuang
hive_location=hdfs://ns1/user/e_ruichuang/private

filedate=${1}
filemonth=`echo ${filedate:0:6}`
beforedate=`date -d "-1 day ${filedate}" +%Y%m%d`
deldate=`date -d "-30 day ${filedate}" +%Y%m%d`

sh tool_out_phone.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate} 1
sh tool_out_mac_area.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}