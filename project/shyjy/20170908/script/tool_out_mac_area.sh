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
                kill_pid tool_out_mac_area.sh
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
                kill_pid tool_out_mac_area.sh
            fi
        fi
    fi
}

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_out_phone_area;
create table traffic_bi_out_phone_area (
pppoe string,
mac string,
tid string,
phone string,
tn1 string,
tn2 string,
firstdate bigint,
lastdate bigint,
dates bigint,
corp string,
area string,
province string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_out_phone_area';

insert overwrite table traffic_bi_out_phone_area
select t3.mac,t3.pppoe,tid,phone,tn1,tn2,firstdate,lastdate,
dates,corp,area,province from (
select t1.username as mac,t2.pppoe as pppoe,tid,phone,tn1,tn2,firstdate,lastdate,
dates,corp,area
from traffic_bi_out_phone t1
left outer join (
select mac,pppoe from traffic_bi_pppoe
) t2 on t1.username = t2.mac
) t3 left outer join gdpi.yueme_mac_area t4
on t3.mac = t4.mac
group by t3.mac,t3.pppoe,tid,phone,tn1,tn2,firstdate,lastdate,
dates,corp,area,province;
exit;
EOF
`"