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

check_success "`cat <<EOF
use ${hive_database};

CREATE TABLE if not exists traffic_bi_phone (
username string,
tid string,
phone string,
terminal_name string,
firstdate bigint,
lastdate bigint,
dates bigint
)
partitioned by (dir string)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_phone';
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

insert overwrite table traffic_bi_phone partition (dir='${filedate}')
select username,tid,phone,terminal_name,firstdate,lastdate,dates from (
select if(t1.username is null,t2.username,t1.username) as username,
if(t1.tid is null,t2.tid,t1.tid) as tid,
if(t1.phone is null,t2.phone,t1.phone) as phone,
case
when t1.terminal_name is not null and t2.terminal_name is not null and t2.terminal_name <> '苹果iPhone' then t2.terminal_name
when t1.terminal_name is not null and t2.terminal_name is not null and t2.terminal_name = '苹果iPhone' then t1.terminal_name
when t1.terminal_name is not null and t2.terminal_name is null then t1.terminal_name
else t2.terminal_name
end as terminal_name,
case
when t1.username is not null and t2.username is not null then t1.firstdate
when t1.username is null and t2.username is not null then t2.firstdate
else t1.firstdate
end as firstdate,
case
when t1.username is not null and t2.username is not null then t2.lastdate
when t1.username is null and t2.username is not null then t2.lastdate
else t1.lastdate
end as lastdate,
case
when t1.username is not null and t2.username is not null then t1.dates + t2.dates
when t1.username is null and t2.username is not null then t2.dates
else t1.dates
end as dates from (
select username,tid,phone,terminal_name,firstdate,lastdate,dates from traffic_bi_phone where dir='${beforedate}'
) t1
full outer join (
select username,tid,phone,terminal_name,firstdate,lastdate,dates from traffic_bi_phone_temp_${filedate}
) t2
on t1.username = t2.username and t1.tid = t2.tid and t1.phone = t2.phone
) a group by username,tid,phone,terminal_name,firstdate,lastdate,dates;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_phone_temp_${filedate};
alter table traffic_bi_phone drop partition (dir<='${deldate}');
exit;
EOF
`"