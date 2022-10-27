#!/bin/bash

# 定义变量
APP=gmall

# 如果输入了日期就按照输入的日期，未输入的情况下日期去当前日期的前一天
if [ -n "$1" ]
then
    do_date=$1
else
    do_date=`date -d "-1 day" +%F`
fi

echo "--------------->日志日期为 $do_date <----------------"
sql="load data inpath '/origin_data/$APP/log/topic_log/$do_date' into table ${APP}.ods_log_inc partition(dt='$do_date');"

hive -e "$sql"