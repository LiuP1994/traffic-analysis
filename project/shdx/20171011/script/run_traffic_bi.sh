#!/bin/sh

datebeg=$1
dateend=$2

beg_s=`date -d "${datebeg}" +%s`
end_s=`date -d "${dateend}" +%s`

while [[ ${beg_s} -le ${end_s} ]]
do
  for q in 1 2 3 4 5 6 7 8 9 10 11 12
  do
    m_date=`date -d @$beg_s +"%Y%m%d"`
    sh traffic_bi.sh ${m_date} ${q} > ../logs/${m_date}_${q}.log 2>&1
  done
  beg_s=$((beg_s+86400))
done
