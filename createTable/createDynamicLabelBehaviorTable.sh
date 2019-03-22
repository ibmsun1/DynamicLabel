#!/bin/bash
v=$1;
env=pro;
hive -e "create table $env"$v".ods_vip_behavior1(vipid bigint,brandId int,copId int,acttype int,couponrate double,couponcount int,avgtimelength double,lastdistancecurrentdays int)STORED AS ORC;"

hive -e "create table $env"$v".ods_vip_behavior2(vipid bigint,brandId int,copId int,acttype int,count int)STORED AS ORC;"
hive -e "create table $env"$v".ods_vip_behavior3(vipid bigint,brandId int,copId int,acttype int,count int)STORED AS ORC;"
