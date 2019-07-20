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
                kill_pid tool_out_terminal.sh
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
                kill_pid tool_out_terminal.sh
            fi
        fi
    fi
}

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_out_termminal_${filedate};
create table traffic_bi_out_termminal_${filedate} (
username string,
tid string,
terminal_name string,
firstdate bigint,
lastdate bigint,
dates bigint
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_out_termminal_${filedate}';

insert overwrite table traffic_bi_out_termminal_${filedate}
select username,tid,terminal_name,firstdate,lastdate,dates from
traffic_bi_terminal where dir='${filedate}';
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_out_termminal_blist${filedate};
create table traffic_bi_out_termminal_blist${filedate} (
tid string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_out_termminal_blist${filedate}';

insert overwrite table traffic_bi_out_termminal_blist${filedate}
select tid from (
select tid,count(terminal_name) as cnt from (
select tid,terminal_name from traffic_bi_out_termminal_${filedate}
group by tid,terminal_name
) t1 group by tid
) t2 where cnt > 1;
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_out_termminal;
create table traffic_bi_out_termminal (
username string,
tid string,
terminal_name string,
firstdate bigint,
lastdate bigint,
dates bigint
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_out_termminal';

set hive.exec.compress.output=false;

insert overwrite table traffic_bi_out_termminal
select username,t1.tid,terminal_name,firstdate,lastdate,dates
from traffic_bi_out_termminal_${filedate} t1 left outer join
traffic_bi_out_termminal_blist${filedate} t2
on t1.tid = t2.tid
where t2.tid is null;
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_out_termminal_${filedate};
drop table traffic_bi_out_termminal_blist${filedate};
exit;
EOF
`"

