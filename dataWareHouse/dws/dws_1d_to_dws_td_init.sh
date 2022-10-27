#!/bin/bash

if [ $# -lt 1 ]
then
  echo "参数个数有误!"
  echo "参数1：必选 同步的表名，同步全部表请输入 all"
  echo "参数2：必选 首日同步的日期"
  exit
fi

APP=gmall

if [ -n "$2" ]
then
  do_date=$2
else
  echo "请输入日期参数"
  exit
fi

dws_trade_user_order_td="
insert overwrite table ${APP}.dws_trade_user_order_td partition (dt='$do_date')
select
    user_id,
    min(dt) order_date_first,
    max(dt) order_date_last,
    sum(order_count_1d) order_count_td,
    sum(order_num_1d) order_num_td,
    sum(order_original_amount_1d) original_amount_td,
    sum(activity_reduce_amount_1d) activity_reduce_amount_td,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_td,
    sum(order_total_amount_1d) total_amount_td
from ${APP}.dws_trade_user_order_1d
group by user_id;"

dws_trade_user_payment_td="
insert overwrite table ${APP}.dws_trade_user_payment_td partition (dt='$do_date')
select
    user_id,
    min(dt) payment_date_first,
    max(dt) payment_date_last,
    sum(payment_count_1d) payment_count_td,
    sum(payment_num_1d) payment_num_td,
    sum(payment_amount_1d) payment_amount_td
from ${APP}.dws_trade_user_payment_1d
group by user_id;"

dws_user_user_login_td="
insert overwrite table ${APP}.dws_user_user_login_td partition (dt='$do_date')
select
    u.id,
    nvl(login_date_last, date_format(create_time, 'yyyy-MM-dd')),
    nvl(login_count_td, 1)
from
    (
        select
            id,
            create_time
        from ${APP}.dim_user_zip
        where dt='9999-12-31'
    ) u
    left join
    (
        select
            user_id,
            max(dt) login_date_last,
            count(*) login_count_td
        from ${APP}.dwd_user_login_inc
        where dt='$do_date'
        group by user_id
     ) l
on u.id = l.user_id;"

case $1 in
  "dws_trade_user_order_td")
    hive -e "$dws_trade_user_order_td"
  ;;
  "dws_trade_user_payment_td")
    hive -e "$dws_trade_user_payment_td"
  ;;
  "dws_user_user_login_td")
    hive -e "$dws_user_user_login_td"
  ;;
  "all")
    hive -e "$dws_trade_user_order_td$dws_trade_user_payment_td$dws_user_user_login_td"
  ;;
esac