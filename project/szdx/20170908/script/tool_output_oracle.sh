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

# 数据库连接配置
DBUSER=bhaman
PASSWD=bhaman
URL=132.108.200.162/dbbha

output_hive_to_oracle(){
  mkdir -p ${rundir}/temp/data/
  cd ${rundir}/temp/data/
	rm ${rundir}/temp/data/*
  hive -e "${1}" > temp.dat
  cat <<EOF > temp.ctl
load data
CHARACTERSET UTF8
INFILE 'temp.dat'
append
into table ${2}
fields terminated by '	'
trailing nullcols
(${3})
EOF
sqlldr ${DBUSER}/${PASSWD}@${URL} control=temp.ctl direct=true bindsize=2097152 readsize=2097152
cat ./temp.log >> ${rundir}/logs/output_${filedate}.log
}

sqlplus ${DBUSER}/${PASSWD}@${URL} <<SQL
drop table traffic_phone;
create table traffic_phone(
username varchar2(1024),
tid varchar2(1024),
phone varchar2(1024),
firstdate number(15),
lastdate number(15),
dates number(15),
terminal_type varchar2(1024),
terminal_brand varchar2(1024),
terminal_name varchar2(1024),
terminal_yidong varchar2(1024),
terminal_liantong varchar2(1024),
terminal_dianxin varchar2(1024),
terminal_dual varchar2(1024),
terminal_age varchar2(1024),
terminal_monty varchar2(1024),
terminal_price varchar2(1024)
);

drop table traffic_terminal;
create table traffic_terminal(
username varchar2(1024),
tid varchar2(1024),
firstdate number(15),
lastdate number(15),
dates number(15),
terminal_type varchar2(1024),
terminal_brand varchar2(1024),
terminal_name varchar2(1024),
terminal_yidong varchar2(1024),
terminal_liantong varchar2(1024),
terminal_dianxin varchar2(1024),
terminal_dual varchar2(1024),
terminal_age varchar2(1024),
terminal_monty varchar2(1024),
terminal_price varchar2(1024)
);
SQL

output_hive_to_oracle "`cat <<EOF
use shkd;
select t1.username,t1.tid,t1.phone,t1.firstdate,t1.lastdate,t1.dates,
t2.terminal_type,t2.terminal_brand,t2.terminal_name,t2.terminal_yidong,
t2.terminal_liantong,t2.terminal_dianxin,t2.terminal_dual,t2.terminal_age,
t2.terminal_monty,t2.terminal_price from traffic_bi_out_phone t1
left outer join traffic_bi_static_terminals t2
on t1.terminal_name = t2.terminal_name;
exit;
EOF
`" "traffic_phone" "username,tid,phone,firstdate,lastdate,dates,terminal_type,terminal_brand,terminal_name,terminal_yidong,terminal_liantong,terminal_dianxin,terminal_dual,terminal_age,terminal_monty,terminal_price"

output_hive_to_oracle "`cat <<EOF
use shkd;
select t1.username,t1.tid,t1.firstdate,t1.lastdate,t1.dates,
t2.terminal_type,t2.terminal_brand,t2.terminal_name,t2.terminal_yidong,
t2.terminal_liantong,t2.terminal_dianxin,t2.terminal_dual,t2.terminal_age,
t2.terminal_monty,t2.terminal_price from traffic_bi_out_terminal t1
left outer join traffic_bi_static_terminals t2
on t1.terminal_name = t2.terminal_name;
exit;
EOF
`" "traffic_terminal" "username,tid,firstdate,lastdate,dates,terminal_type,terminal_brand,terminal_name,terminal_yidong,terminal_liantong,terminal_dianxin,terminal_dual,terminal_age,terminal_monty,terminal_price"
