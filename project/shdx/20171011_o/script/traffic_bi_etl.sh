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
                kill_pid traffic_bi.sh
                kill_pid traffic_bi_etl.sh
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
                kill_pid traffic_bi_etl.sh
            fi
        fi
    fi
}


check_success "`cat <<EOF
use ${hive_database};

drop table traffic_bi_tmp_userinfo_${filedate};
create table traffic_bi_tmp_userinfo_${filedate} (
username string,
url string,
host string,
ua string,
cookie string,
stattime string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_tmp_userinfo_${filedate}';

set hive.exec.compress.output=true;
set mapred.output.compress=true;    
set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec; 
set mapreduce.map.failures.maxpercent=90;
set mapreduce.reduce.failures.maxpercent=90;

insert overwrite table traffic_bi_tmp_userinfo_${filedate}
select username,url,host,ua,cookie,stattime from (
select ad as username,url as url,parse_url(url,'HOST') as host, ua as ua,cookie as cookie,
ts as stattime from gdpi.sada_new_click
where datelabel='${filedate}'
) t2;
exit;
EOF
`"


check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_decode_url_${filedate};
CREATE TABLE traffic_bi_decode_url_${filedate} (
username string,
url string,
host string,
ua string,
cookie string,
stattime string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_decode_url_${filedate}';
exit;
EOF
`"

if [[ ${unicode} -eq 1 ]]; then
check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.UrlDecode ${hive_location}/traffic_bi_tmp_userinfo_${filedate} ${hive_location}/traffic_bi_decode_url_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime 1
EOF
`" "hadoop fs -rm -r ${hive_location}/traffic_bi_decode_url_${filedate}"
else
check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.UrlDecode ${hive_location}/traffic_bi_tmp_userinfo_${filedate} ${hive_location}/traffic_bi_decode_url_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime
EOF
`" "hadoop fs -rm -r ${hive_location}/traffic_bi_decode_url_${filedate}"
fi

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_tmp_userinfo_${filedate};
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_mobile_info_${filedate};
create table traffic_bi_mobile_info_${filedate} (
username string,
url string,
host string,
ua string,
cookie string,
stattime string,
phone string,
imei string,
imsi string,
mac string,
idfa string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_mobile_info_${filedate}';
exit;
EOF
`"

check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.MobileInfo  ${hive_location}/traffic_bi_decode_url_${filedate} ${hive_location}/traffic_bi_mobile_info_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime
EOF
`" "hadoop fs -rm -r ${hive_location}/traffic_bi_mobile_info_${filedate}"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_decode_url_${filedate};
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_parser_ua_${filedate};
create table traffic_bi_parser_ua_${filedate} (
username string,
url string,
host string,
ua string,
cookie string,
stattime string,
phone string,
imei string,
imsi string,
mac string,
idfa string,
terminal string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_parser_ua_${filedate}';
exit;
EOF
`"

hadoop fs -rm -r ${hive_location}/traffic_bi_static_ua
hadoop fs -put ${rundir}/static/${traffic_bi_static_ua} ${hive_location}/traffic_bi_static_ua

check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.Useragent  ${hive_location}/traffic_bi_mobile_info_${filedate} ${hive_location}/traffic_bi_static_ua ${hive_location}/traffic_bi_parser_ua_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa
EOF
`" "hadoop fs -rm -r ${hive_location}/traffic_bi_parser_ua_${filedate}"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_mobile_info_${filedate};
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_match_terminal_name_${filedate};
create table traffic_bi_match_terminal_name_${filedate} (
username string,
url string,
host string,
ua string,
cookie string,
stattime string,
phone string,
imei string,
imsi string,
mac string,
idfa string,
terminal string,
terminal_name string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_match_terminal_name_${filedate}';
exit;
EOF
`"

hadoop fs -rm -r ${hive_location}/traffic_bi_static_terminal_detail
hadoop fs -put ${rundir}/static/${traffic_bi_static_terminal_detail} ${hive_location}/traffic_bi_static_terminal_detail

hadoop fs -rm -r ${hive_location}/traffic_bi_id_iphone
hadoop fs -put ${rundir}/static/${traffic_bi_id_iphone} ${hive_location}/traffic_bi_id_iphone

check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.TerminalName  ${hive_location}/traffic_bi_parser_ua_${filedate} ${hive_location}/traffic_bi_static_terminal_detail ${hive_location}/traffic_bi_id_iphone ${hive_location}/traffic_bi_match_terminal_name_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa,11,terminal
EOF
`" "hadoop fs -rm -r ${hive_location}/traffic_bi_match_terminal_name_${filedate}"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_parser_ua_${filedate};
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_host_match_${filedate};
create table traffic_bi_host_match_${filedate} (
username string,
url string,
host string,
ua string,
cookie string,
stattime string,
phone string,
imei string,
imsi string,
terminal string,
terminal_name string,
hostname string,
hosttype string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_host_match_${filedate}';
exit;
EOF
`"
hadoop fs -rm -r ${hive_location}/traffic_bi_static_host
hadoop fs -put ${rundir}/static/${traffic_bi_static_host} ${hive_location}/traffic_bi_static_host

check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.HostMatcher  ${hive_location}/traffic_bi_match_terminal_name_${filedate} ${hive_location}/traffic_bi_static_host ${hive_location}/traffic_bi_host_match_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa,11,terminal,12,terminal_name
EOF
`" "hadoop fs -rm -r ${hive_location}/traffic_bi_host_match_${filedate}"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_match_terminal_name_${filedate};
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_id_catch_${filedate};
create table traffic_bi_id_catch_${filedate} (
username string,
url string,
host string,
ua string,
cookie string,
stattime string,
phone string,
imei string,
imsi string,
mac string,
idfa string,
terminal string,
terminal_name string,
hostname string,
hosttype string,
uid string,
field1 string,
field2 string,
field3 string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_id_catch_${filedate}';
exit;
EOF
`"
hadoop fs -rm -r ${hive_location}/traffic_bi_id_catch
hadoop fs -put ${rundir}/static/${traffic_bi_id_catch} ${hive_location}/traffic_bi_id_catch

check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.IdCatch  ${hive_location}/traffic_bi_host_match_${filedate} ${hive_location}/traffic_bi_id_catch ${hive_location}/traffic_bi_id_catch_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa,11,terminal,12,terminal_name,13,hostname,14,hosttype
EOF
`" "hadoop fs -rm -r ${hive_location}/traffic_bi_id_catch_${filedate}"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_host_match_${filedate};
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_id_catch_${filedate}_Id2Phone;
create table traffic_bi_id_catch_${filedate}_Id2Phone (
username string,
tid string,
phone string,
terminal_name string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_id_catch_${filedate}_Id2Phone';

drop table traffic_bi_id_catch_${filedate}_Id2Uid;
create table traffic_bi_id_catch_${filedate}_Id2Uid (
username string,
tid string,
uid string,
terminal_name string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_id_catch_${filedate}_Id2Uid';

drop table traffic_bi_id_catch_${filedate}_Id2Terminal;
create table traffic_bi_id_catch_${filedate}_Id2Terminal (
username string,
tid string,
terminal_name string
)
row format delimited
fields terminated by '\t'
STORED AS TEXTFILE
location '${hive_location}/traffic_bi_id_catch_${filedate}_Id2Terminal';
exit;
EOF
`"

check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.idmapping.IdMapping ${hive_location}/traffic_bi_id_catch_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa,11,terminal,12,terminal_name,13,hostname,14,hosttype,15,uid
EOF
`" ""

check_success "`cat <<EOF
use ${hive_database};
drop table traffic_bi_id_catch_${filedate};
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};

create table if not exists traffic_bi_etl_userinfo (
username string,
url string,
host string,
ua string,
cookie string,
stattime string,
phone string,
imei string,
imsi string,
mac string,
idfa string,
terminal string,
terminal_name string,
hostname string,
hosttype string,
uid string,
field1 string,
field2 string,
field3 string
)
partitioned by (dir string)
row format delimited
fields terminated by '\t'
location '${hive_location}/traffic_bi_etl_userinfo';
exit;
EOF
`"


check_success "`cat <<EOF
use ${hive_database};
alter table traffic_bi_etl_userinfo drop partition (dir='${filedate}');

insert into table traffic_bi_etl_userinfo partition (dir='${filedate}')
select
username,url,host,ua,cookie,stattime,phone,
imei,imsi,mac,idfa,terminal,terminal_name,hostname,hosttype,uid,field1,
field2,field3
from
traffic_bi_id_catch_${filedate};
exit;
EOF
`"

check_success "`cat <<EOF
use ${hive_database};
alter table traffic_bi_etl_userinfo drop partition (dir='${deldate}');
exit;
EOF
`"