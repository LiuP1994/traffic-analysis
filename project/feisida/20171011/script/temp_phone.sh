#!/usr/bin/env bash
rundir=${1}
bigdataJar=${2}
hive_database=${3}
hive_location=${4}
filedate=${5}
filemonth=${6}
beforedate=${7}
deldate=${8}
q=${9}

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
                kill_pid traffic_bi.sh
                kill_pid traffic_bi_phone.sh
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
                kill_pid traffic_bi.sh
                kill_pid traffic_bi_phone.sh
            fi
        fi
    fi
}

if [[ ${q} -eq 1 ]]; then
check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_phone_temp_${filedate};
create table traffic_bi_phone_temp_${filedate} (
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
location '${hive_location}/traffic_bi_phone_temp_${filedate}';
EOF
`"
fi

check_success "`cat <<EOF
use ${hive_database};

insert into table traffic_bi_phone_temp_${filedate}
select username,tid,phone,terminal_name,unix_timestamp('${filedate} 00:00:00','yyyyMMdd HH:mm:ss') as firstdate,
unix_timestamp('${filedate} 00:00:00','yyyyMMdd HH:mm:ss') as lastdate,1 as dates
from traffic_bi_id_catch_${filedate}_Id2Phone;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_id_catch_${filedate}_id2phone;
exit;
EOF
`"