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
                kill_pid tmp_phone_host.sh
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
                kill_pid tmp_phone_host.sh
            fi
        fi
    fi
}


check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_phone_host_temp2;
create table if not exists traffic_bi_phone_host_temp2 (
username string,
host string,
phone string
)
row format delimited
fields terminated by '\t'
location '${hive_location}/traffic_bi_phone_host_temp2';

insert overwrite table traffic_bi_phone_host_temp2
select username,host,phone from traffic_bi_etl_userinfo
where dir='${filedate}' and phone <> ''
and username rlike '^([0-9a-fA-F]{2})(([:-][0-9a-fA-F]{2}){5})$'
group by username,host,phone;
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_phone_host_temp;
create table traffic_bi_phone_host_temp (
username string,
host string,
phone string,
corp string,
area string
)
row format delimited
fields terminated by '\t'
location '${hive_location}/traffic_bi_phone_host_temp';
exit;
EOF
`"
hadoop fs -rm -r ${hive_location}/traffic_bi_static_phone_head
hadoop fs -put ${rundir}/static/traffic_bi_static_phone_head_unicode.txt ${hive_location}/traffic_bi_static_phone_head


hadoop fs -rm -r ${hive_location}/traffic_bi_phone_host_temp
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.CorpArea  ${hive_location}/traffic_bi_phone_host_temp2 ${hive_location}/traffic_bi_static_phone_head ${hive_location}/traffic_bi_phone_host_temp 0,username,1,host,2,phone

check_success "`cat <<EOF
use ${hive_database};

create table  if not exists traffic_bi_phone_host (
username string,
host string,
phone string,
corp string,
area string
)
partitioned by (dir string)
row format delimited
fields terminated by '\t'
location '${hive_location}/traffic_bi_phone_host';

insert overwrite table traffic_bi_phone_host partition (dir='${filedate}')
select username,host,phone,corp,area from (
select if(t1.username is not null,t1.username,t2.username) as username,
if(t1.host is not null,t1.host,t2.host) as host,
if(t1.phone is not null,t1.phone,t2.phone) as phone,
if(t1.corp is not null,t1.corp,t2.corp) as corp,
if(t1.area is not null,t1.area,t2.area) as area 
from (
select username,host,phone,corp,area from traffic_bi_phone_host where dir='${beforedate}'
and username <> '00:00:00:00:00:00'
) t1
full outer join (
select username,host,phone,corp,area from traffic_bi_phone_host_temp where username <> '00:00:00:00:00:00'
) t2
on t1.username = t2.username and t1.host = t2.host and t1.phone = t2.phone
) a group by username,host,phone,corp,area;
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_check_score_phone;
create table traffic_bi_check_score_phone (
host string,
acnt string,
bcnt string,
score string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_check_score_phone';

insert overwrite table traffic_bi_check_score_phone
select host,bcnt,acnt,bcnt/acnt from (
select if(ahost is not null,ahost,bhost) as host,
if(bcnt is not null,bcnt,1) as bcnt,
if(acnt is not null,acnt,1) as acnt from (
select d1.host as ahost,d1.cnt as acnt,d2.host as bhost,d2.cnt as bcnt from (
select host,count(phone) cnt from (
select username,host,phone,corp,area from (
select username,host,phone,corp,area from traffic_bi_phone_host
where dir='${filedate}' and username <> '00:00:00:00:00:00' and username rlike '^([0-9a-fA-F]{2})(([:-][0-9a-fA-F]{2}){5})$'
and (area <= '0710' or area >= '0728')
) a where area <> '027'
) t1 group by host
) d1
full outer join (
select host,cnt from (
select host,count(phone) cnt from (
select username,host,phone,corp,area from traffic_bi_phone_host
where dir='${filedate}' and username <> '00:00:00:00:00:00' and username rlike '^([0-9a-fA-F]{2})(([:-][0-9a-fA-F]{2}){5})$'
and ((area = '027') or area >= '0710' and area <= '0728')
) t1 group by host
) t2
) d2
on  d1.host = d2.host
) a
) b;
exit;
EOF
`"

#check_success "`cat <<EOF
#use ${hive_database};
#
#drop table traffic_bi_out_phone_tmp;
#create table traffic_bi_out_phone_tmp (
#username string,
#tid string,
#phone string,
#tn1 string,
#tn2 string,
#firstdate bigint,
#lastdate bigint,
#dates bigint,
#corp string,
#area string
#)
#row format delimited
#fields terminated by '\t'
#STORED AS TEXTFILE
#location '${hive_location}/traffic_bi_out_phone_tmp';
#
#insert overwrite table traffic_bi_out_phone_tmp
#select username,tid,b1.phone,tn1,tn2,firstdate,lastdate,dates,corp,area from traffic_bi_out_phone b1
#left outer join (
#select phone,t1.host as host from (
#select aphone as phone,host from (
#select phone,host from traffic_bi_phone_host where dir=${filedate}
#) a lateral view explode(split(phone, ',')) myTable as aphone 
#) t1
#left outer join (
#select host from traffic_bi_check_score_phone where score >= 0.5
#) t2
#on t1.host = t2.host
#where t2.host is not null
#) b2 on b1.phone = b2.phone where b2.phone is not null;
#exit;
#EOF
#`"

check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_phone_host_temp2;
drop table traffic_bi_phone_host_temp;
alter table traffic_bi_phone_host drop partition (dir<='${deldate}');
exit;
EOF
`"
