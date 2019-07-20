#!/usr/bin/env bash
rundir=/home/e_ruichuang/shyjy/20170908
bigdataJar=${rundir}/analysis-1.0-SNAPSHOT.jar
hive_database=e_ruichuang
hive_location=hdfs://ns1/user/e_ruichuang/private

filedate=${1}
filemonth=`echo ${filedate:0:6}`
beforedate=`date -d "-1 day ${filedate}" +%Y%m%d`
deldate=`date -d "-30 day ${filedate}" +%Y%m%d`
unicode=1

traffic_bi_static_ua=traffic_bi_static_ua.txt
traffic_bi_static_terminal_detail=traffic_bi_static_terminal_detail.txt
traffic_bi_id_iphone=traffic_bi_id_iphone
traffic_bi_static_phone_head=traffic_bi_static_phone_head.txt
traffic_bi_static_host=traffic_bi_static_host.txt
traffic_bi_id_catch=traffic_bi_id_catch.txt

if [[ ${unicode} -eq 1 ]]; then
    traffic_bi_static_ua=traffic_bi_static_ua_unicode.txt
    traffic_bi_static_terminal_detail=traffic_bi_static_terminal_detail_unicode.txt
    traffic_bi_static_phone_head=traffic_bi_static_phone_head_unicode.txt
    traffic_bi_static_host=traffic_bi_static_host_unicode.txt
    traffic_bi_id_catch=traffic_bi_id_catch_unicode.txt
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
                kill_pid traffic_bi_phone_host2.sh
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
                kill_pid traffic_bi_phone_host2.sh
            fi
        fi
    fi
}


check_success "`cat <<EOF
use ${hive_database};

create table if not exists traffic_bi_phone_host (
username string,
host string,
phone string
)
partitioned by (dir string)
row format delimited
fields terminated by '\t'
location '${hive_location}/traffic_bi_phone_host';

insert overwrite table traffic_bi_phone_host partition (dir='${filedate}')
select username,host,phone from (
select if(t1.username is not null,t1.username,t2.username) as username,
if(t1.host is not null,t1.host,t2.host) as host,
if(t1.phone is not null,t1.phone,t2.phone) as phone from (
select username,host,phone from traffic_bi_phone_host where dir='${beforedate}'
and username <> '00:00:00:00:00:00'
) t1
full outer join (
select username,host,phone from traffic_bi_phone_host_temp where dir='${filedate}'
and username <> '00:00:00:00:00:00'
) t2
on t1.username = t2.username and t1.host = t2.host and t1.phone = t2.phone
) a group by username,host,phone;
exit;
EOF
`"
