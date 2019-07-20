#!/usr/bin/env bash

rundir=${1}
bigdataJar=${2}
hive_database=${3}
hive_location=${4}
filedate=${5}
filemonth=${6}
beforedate=${7}
deldate=${8}
unicode=${9}

traffic_bi_static_phone_head=traffic_bi_static_phone_head.txt

if [[ ${unicode} -eq 1 ]]; then
    traffic_bi_static_phone_head=traffic_bi_static_phone_head_unicode.txt
fi
kill_pid(){
    ps -aux | grep "${1}" | grep -v "grep" | awk '{print $2}' | while read line;
    do
        kill -9 $line
    done
}

retry=2
sleep_time=5
retry_times=0

check_success(){
    if [[ $# = 1 ]]; then
        local run_sql=${1}
        hive -e "${run_sql}" -v
        local l_run_type=`echo $?`
        check_success "${l_run_type}" "${run_sql}"
    else
        local run_type=${1}
        local run_sql=${2}
        if [[ ${run_type} -eq 0 ]]; then
            retry_times=0
            local log="[info] ${run_sql}"
            echo ${log}
        else
            if [[ ${retry_times} -lt ${retry} ]]; then
                local log="[error] ${run_sql}"
                echo ${log}
                sleep ${sleep_time}
                retry_times=$(($retry_times + 1))
                hive -e "${run_sql}" -v
                local l_run_type=`echo $?`
                check_success "${l_run_type}" "${run_sql}"
            else
                local log="[fail] ${run_sql}"
                echo ${log}
                kill_pid tool_out.sh
                kill_pid tool_out_phone.sh
            fi
        fi
    fi
}

check_success_mr(){
    if [[ $# = 2 ]]; then
        local run_mr=${1}
		local rm_output=${2}
		${rm_output}
        ${run_mr}
        local l_run_type=`echo $?`
        check_success_mr "${l_run_type}" "${run_mr}" "${rm_output}"
    else
        local run_type=${1}
        local run_mr=${2}
		local rm_output=${3}
        if [[ ${run_type} -eq 0 ]]; then
            retry_times=0
            local log="[info] ${run_mr}"
            echo ${log}
        else
            if [[ ${retry_times} -lt ${retry} ]]; then
                local log="[error] ${run_mr}"
                echo ${log}
                sleep ${sleep_time}
                retry_times=$(($retry_times + 1))
				${rm_output}
                ${run_mr}
                local l_run_type=`echo $?`
                check_success_mr "${l_run_type}" "${run_mr}" "${rm_output}"
            else
                local log="[fail] ${run_mr}"
                echo ${log}
                kill_pid tool_out.sh
                kill_pid tool_out_phone.sh
            fi
        fi
    fi
}

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_out_phone_${filedate};
create table traffic_bi_out_phone_${filedate} (
username string,
tid string,
phone string,
terminal_name string,
firstdate bigint,
lastdate bigint,
dates bigint
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_out_phone_${filedate}';

insert overwrite table traffic_bi_out_phone_${filedate}
select username,tid,phone,terminal_name,firstdate,lastdate,dates from
traffic_bi_phone where dir='${filedate}' and dates > 1 and username <> '00:00:00:00:00:00';
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_out_phone_blist${filedate};
create table traffic_bi_out_phone_blist${filedate} (
tid string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_out_phone_blist${filedate}';

insert overwrite table traffic_bi_out_phone_blist${filedate}
select tid from (
select tid,count(phone) as cnt from (
select tid,phone from traffic_bi_out_phone_${filedate}
group by tid,phone
) t1 group by tid
) t2 where cnt > 5;
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_out_phone_${filedate}_2;
create table traffic_bi_out_phone_${filedate}_2 (
username string,
tid string,
phone string,
tn1 string,
tn2 string,
firstdate bigint,
lastdate bigint,
dates bigint
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_out_phone_${filedate}_2';

set hive.exec.compress.output=false;

insert overwrite table traffic_bi_out_phone_${filedate}_2
select username,t1.tid,phone,regexp_replace(terminal_name,'[（\\(].*','') as tn1,terminal_name as tn2,firstdate,lastdate,dates
from traffic_bi_out_phone_${filedate} t1 left outer join
traffic_bi_out_phone_blist${filedate} t2
on t1.tid = t2.tid
where t2.tid is null;
exit;
EOF
`"


check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_out_phone;
create table traffic_bi_out_phone (
username string,
tid string,
phone string,
tn1 string,
tn2 string,
firstdate bigint,
lastdate bigint,
dates bigint,
corp string,
area string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_out_phone';
exit;
EOF
`"

hadoop fs -rm -r ${hive_location}/traffic_bi_static_phone_head
hadoop fs -put ${rundir}/static/${traffic_bi_static_phone_head} ${hive_location}/traffic_bi_static_phone_head

hadoop fs -rm -r ${hive_location}/traffic_bi_out_phone
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.CorpArea  ${hive_location}/traffic_bi_out_phone_${filedate}_2 ${hive_location}/traffic_bi_static_phone_head ${hive_location}/traffic_bi_out_phone 0,username,1,tid,2,phone,3,tn1,4,tn2,5,firstdate,6,lastdate,7,dates

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_out_phone_${filedate};
drop table traffic_bi_out_phone_${filedate}_2;
drop table traffic_bi_out_phone_blist${filedate};
exit;
EOF
`"