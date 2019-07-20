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

#traffic_bi_static_ua=traffic_bi_static_ua.txt
#traffic_bi_static_terminal_detail=traffic_bi_static_terminal_detail.txt
traffic_bi_id_iphone=traffic_bi_id_iphone
#traffic_bi_static_phone_head=traffic_bi_static_phone_head.txt
#traffic_bi_static_host=traffic_bi_static_host.txt
#traffic_bi_id_catch=traffic_bi_id_catch.txt


traffic_bi_static_ua=traffic_bi_static_ua_unicode.txt
traffic_bi_static_terminal_detail=traffic_bi_static_terminal_detail_unicode.txt
traffic_bi_static_phone_head=traffic_bi_static_phone_head_unicode.txt
traffic_bi_static_host=traffic_bi_static_host_unicode.txt
traffic_bi_id_catch=traffic_bi_id_catch_unicode.txt


kill_pid(){
    ps -aux | grep "${1}" | grep -v "grep" | awk '{print $2}' | while read line;
    do  
        kill -9 $line  
    done 
}

retry=5
sleep_time=18000
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

timelabel=""
if [[ ${q} -eq 1 ]]; then
	timelabel="timelabel between '00' and '01'"
fi
if [[ ${q} -eq 2 ]]; then
	timelabel="timelabel between '02' and '03'"
fi
if [[ ${q} -eq 3 ]]; then
	timelabel="timelabel between '04' and '05'"
fi
if [[ ${q} -eq 4 ]]; then
	timelabel="timelabel between '06' and '07'"
fi
if [[ ${q} -eq 5 ]]; then
	timelabel="timelabel between '08' and '09'"
fi
if [[ ${q} -eq 6 ]]; then
	timelabel="timelabel between '10' and '11'"
fi
if [[ ${q} -eq 7 ]]; then
	timelabel="timelabel between '12' and '13'"
fi
if [[ ${q} -eq 8 ]]; then
	timelabel="timelabel between '14' and '15'"
fi
if [[ ${q} -eq 9 ]]; then
	timelabel="timelabel between '16' and '17'"
fi
if [[ ${q} -eq 10 ]]; then
	timelabel="timelabel between '18' and '19'"
fi
if [[ ${q} -eq 11 ]]; then
	timelabel="timelabel between '20' and '21'"
fi
if [[ ${q} -eq 12 ]]; then
	timelabel="timelabel between '22' and '23'"
fi

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
select f13 as ip,f15 as port,concat('http://',f45,f46) as url,f45 as host,f48 as ua,f47 as cookie,f6 as stattime from o_data.u_data_dpi where dir='${filedate}' and (${timelabel}) and f46 is not null and f46 <> '' 
) t1
left outer join (
select n1 as username,ip2 as ip,n3 as minport,n4 as maxport from (
select n1,ip2,n3,n4 from o_data.traffic_o_data_r${filedate}
group by n1,ip2,n3,n4
) t_rds group by n1,ip2,n3,n4
) t2
on t1.ip = t2.ip
where t2.username is not null 
and t2.ip is not null 
and t1.port between t2.minport and t2.maxport;
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
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.UrlDecode -D mapreduce.map.failures.maxpercent=90 -D mapreduce.reduce.failures.maxpercent=90 ${hive_location}/traffic_bi_tmp_userinfo_${filedate} ${hive_location}/traffic_bi_decode_url_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime 1
EOF
`" "hadoop fs -rm -r ${hive_location}/traffic_bi_decode_url_${filedate}"
else
check_success_mr "`cat <<EOF
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.UrlDecode -D mapreduce.map.failures.maxpercent=90 -D mapreduce.reduce.failures.maxpercent=90 ${hive_location}/traffic_bi_tmp_userinfo_${filedate} ${hive_location}/traffic_bi_decode_url_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime
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
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.MobileInfo -D mapreduce.map.failures.maxpercent=90 -D mapreduce.reduce.failures.maxpercent=90 ${hive_location}/traffic_bi_decode_url_${filedate} ${hive_location}/traffic_bi_mobile_info_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime
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
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.Useragent -D mapreduce.map.failures.maxpercent=90 -D mapreduce.reduce.failures.maxpercent=90 ${hive_location}/traffic_bi_mobile_info_${filedate} ${hive_location}/traffic_bi_static_ua ${hive_location}/traffic_bi_parser_ua_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa
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
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.TerminalName -D mapreduce.map.failures.maxpercent=90 -D mapreduce.reduce.failures.maxpercent=90 ${hive_location}/traffic_bi_parser_ua_${filedate} ${hive_location}/traffic_bi_static_terminal_detail ${hive_location}/traffic_bi_id_iphone ${hive_location}/traffic_bi_match_terminal_name_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa,11,terminal
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
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.HostMatcher -D mapreduce.map.failures.maxpercent=90 -D mapreduce.reduce.failures.maxpercent=90 ${hive_location}/traffic_bi_match_terminal_name_${filedate} ${hive_location}/traffic_bi_static_host ${hive_location}/traffic_bi_host_match_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa,11,terminal,12,terminal_name
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
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.etl.IdCatch -D mapreduce.map.failures.maxpercent=90 -D mapreduce.reduce.failures.maxpercent=90 ${hive_location}/traffic_bi_host_match_${filedate} ${hive_location}/traffic_bi_id_catch ${hive_location}/traffic_bi_id_catch_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa,11,terminal,12,terminal_name,13,hostname,14,hosttype
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
hadoop jar ${bigdataJar} cn.com.runtrend.analysis.hadoop.idmapping.IdMapping -D mapreduce.map.failures.maxpercent=90 -D mapreduce.reduce.failures.maxpercent=90 ${hive_location}/traffic_bi_id_catch_${filedate} 0,username,1,url,2,host,3,ua,4,cookie,5,stattime,6,phone,7,imei,8,imsi,9,mac,10,idfa,11,terminal,12,terminal_name,13,hostname,14,hosttype,15,uid
EOF
`" ""

hadoop fs -rm -r ${hive_location}/traffic_bi_id_catch_${filedate}_id1
