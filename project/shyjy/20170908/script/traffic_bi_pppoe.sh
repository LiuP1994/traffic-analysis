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
                kill_pid traffic_bi_pppoe.sh
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
                kill_pid traffic_bi_pppoe.sh
            fi
        fi
    fi
}

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_pppoe_${filedate};
CREATE TABLE traffic_bi_pppoe_${filedate} (
mac string,
pppoe string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_pppoe_${filedate}';
set hive.exec.compress.output=false;
insert overwrite table traffic_bi_pppoe_${filedate}
select mac,pppoe from (
select mac,ad as pppoe from gdpi.yueme_gdpi_http
where datelabel='${filedate}' and province='42' and ad <> '' and ad is not null and ad <> '(null)'
) a group by mac,pppoe;
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

create table if not exists traffic_bi_pppoe (
mac string,
pppoe string
)
partitioned by (dir string)
row format delimited
fields terminated by '\t'
location '${hive_location}/traffic_bi_pppoe';

alter table traffic_bi_pppoe drop partition (dir='${filedate}');

insert into table traffic_bi_pppoe partition (dir='${filedate}')
select if(t1.mac is not null,t1.mac,t2.mac) as mac,
if(t1.pppoe is not null,t1.pppoe,t2.pppoe) as pppoe
from traffic_bi_pppoe_${filedate} t1
full outer join (
select mac,pppoe from traffic_bi_pppoe where dir='${beforedate}'
)t2 on t1.mac = t2.mac and t1.pppoe = t2.pppoe;
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_pppoe_${filedate};
alter table traffic_bi_pppoe drop partition (dir='${deldate}');
exit;
EOF
`"