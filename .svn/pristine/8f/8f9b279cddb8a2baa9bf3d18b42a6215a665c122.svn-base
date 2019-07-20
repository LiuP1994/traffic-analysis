#!/usr/bin/env bash
if [ $# != 1 ]; then
echo "Please enter the target date like 20130901"
exit 1
fi

rundir=/var/lib/hadoop-hdfs/zqq/test
bigdataJar=${rundir}/analysis-1.0-SNAPSHOT.jar
hive_database=cftest
hive_location=/user/hive/warehouse/cftest.db

filedate=${1}
filemonth=`echo ${filedate:0:6}`
beforedate=`date -d "-1 day ${filedate}" +%Y%m%d`
deldate=`date -d "-30 day ${filedate}" +%Y%m%d`

sh traffic_bi_static.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
sh traffic_bi_etl.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate} 0
sh traffic_bi_phone.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
sh traffic_bi_terminal.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
sh traffic_bi_idmapping.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
