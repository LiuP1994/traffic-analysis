#!/usr/bin/env bash
rundir=${1}
bigdataJar=${2}
hive_database=${3}
hive_location=${4}
filedate=${5}
filemonth=${6}
beforedate=${7}
deldate=${8}

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
                kill_pid traffic_bi_static.sh
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
                kill_pid traffic_bi_static.sh
            fi
        fi
    fi
}

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_static_terminals;
create table traffic_bi_static_terminals (
terminal_type string,
terminal_brand string,
terminal_ebrand string,
terminal_name string,
terminal_oname string,
terminal_yidong string,
terminal_liantong string,
terminal_dianxin string,
terminal_dual string,
terminal_age string,
terminal_monty string,
terminal_price string
)
row format delimited
fields terminated by '\t'
location '${hive_location}/traffic_bi_terminals'
;
load data local inpath "${rundir}/static/traffic_bi_static_terminal_detail.txt" into table  traffic_bi_static_terminals;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_static_host;
create table traffic_bi_static_host (
hosttype string,
hostname string,
host string
)
row format delimited
fields terminated by '\t'
location '${hive_location}/traffic_bi_host'
;
load data local inpath "${rundir}/static/traffic_bi_static_host.txt" into table  traffic_bi_static_host;
EOF
`"