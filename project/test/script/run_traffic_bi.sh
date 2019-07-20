#!/bin/sh

datebeg=$1
dateend=$2

beg_s=`date -d "${datebeg}" +%s`
end_s=`date -d "${dateend}" +%s`

while [[ ${beg_s} -le ${end_s} ]]
do
  m_date=`date -d @$beg_s +"%Y%m%d"`
        sh traffic_bi.sh ${m_date} > ../logs/${m_date}.log 2>&1
  beg_s=$((beg_s+86400))
done
