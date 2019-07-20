#!/usr/bin/env bash
if [ $# != 2 ]; then
   echo "Please enter the target date like 20130901"
   exit 1
fi

rundir=/ndata/20171011
bigdataJar=${rundir}/analysis-1.0-SNAPSHOT.jar
hive_database=e_ruichuang
hive_location=/user/hive/warehouse/e_ruichuang.db


filedate=${1}
filemonth=`echo ${filedate:0:6}`
beforedate=`date -d "-1 day ${filedate}" +%Y%m%d`
deldate=`date -d "-30 day ${filedate}" +%Y%m%d`

q=${2}

#hadoop fs -rm -r /user/u_lx_data/.Trash/*/traffic*

#hadoop fs -rm -r /user/u_lx_data/.Trash/*/tmp*

#hadoop fs -rm -r /user/u_lx_data/.Trash/Current/user/u_lx_data/private/zhangqm/*/traffic*

if [[ ${q} -eq 1 ]]; then
  sh traffic_bi_static.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
fi

sh traffic_bi_etl.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate} 0 ${q}
sh traffic_bi_check.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate} 0 ${q}
sh temp_phone.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate} ${q}
sh temp_terminal.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate} ${q}
sh temp_idmapping.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate} ${q}

if [[ ${q} -eq 12 ]]; then
  sh traffic_bi_phone.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
  sh traffic_bi_terminal.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
  sh traffic_bi_idmapping.sh ${rundir} ${bigdataJar} ${hive_database} ${hive_location} ${filedate} ${filemonth} ${beforedate} ${deldate}
fi

