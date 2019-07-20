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
q=${10}

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

CREATE TABLE if not exists traffic_bi_check_terminal_${filedate} (
terminal string,
cnt bigint
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_check_terminal_${filedate}';
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

insert overwrite table traffic_bi_check_terminal_${filedate}
select terminal,cnt from (
select if(t2.terminal is not null,t2.terminal,t3.terminal) as terminal,
case
when t2.terminal is not null and t3.terminal is not null then t2.cnt + t3.cnt
when t2.terminal is null and t3.terminal is not null then t3.cnt
else t2.cnt
end as cnt from (
select terminal,count(terminal) as cnt from (
select terminal from traffic_bi_id_catch_${filedate} where terminal_name = '' and terminal <> ''
group by terminal
) t1 group by terminal
) t2 full outer join traffic_bi_check_terminal_${filedate} t3
on t2.terminal = t3.terminal
) group by terminal,cnt;

drop table traffic_bi_id_catch_${filedate};
exit;
EOF
`"

if [[ ${q} -eq 12 ]]; then
hive -e "use ${hive_database};
select * from  traffic_bi_check_terminal_${filedate} order by cnt desc limit 500;exit;" > ../check_terminal_${filedate}.txt

hive -e "use ${hive_database};
drop table traffic_bi_check_terminal_${filedate};exit;
"
fi