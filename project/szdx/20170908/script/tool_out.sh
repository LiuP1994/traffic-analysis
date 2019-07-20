#!/usr/bin/env bash
if [ $# != 1 ]; then
echo "Please enter the target date like 20130901"
exit 1
fi

rundir=/home/shkd/20170908
bigdataJar=${rundir}/analysis-1.0-SNAPSHOT.jar
hive_database=shkd
hive_location=/user/hive/warehouse/shkd.db

filedate=${1}
filemonth=`echo ${filedate:0:6}`
beforedate=`date -d "-1 day ${filedate}" +%Y%m%d`
deldate=`date -d "-30 day ${filedate}" +%Y%m%d`

sh tool_out_phone.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate} 0
sh tool_out_terminal.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
sh tool_output_oracle.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}

