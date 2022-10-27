#!/bin/bash

if [ $# -lt 1 ]
then
  echo "参数个数有误!"
  echo "参数1：必选 同步的表名，同步全部表请输入 all"
  echo "参数2：同步的日期，为输入为获取当前日期的前一日"
fi

APP=gmall

if [ -n "$2" ]
then
  do_date=$2
else
  do_date=`date -d '-1 day' +%F`
fi

dws_trade_user_sku_order_nd="
insert overwrite table ${APP}.dws_trade_user_sku_order_nd partition (dt='$do_date')
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(dt >= date_sub('$do_date', 6),order_count_1d, 0)) order_count_7d,
    sum(if(dt >= date_sub('$do_date', 6),order_num_1d, 0)) order_num_7d,
    sum(if(dt >= date_sub('$do_date', 6),order_original_amount_1d, 0)) order_original_amount_7d,
    sum(if(dt >= date_sub('$do_date', 6),activity_reduce_amount_1d, 0)) activity_reduce_amount_7d,
    sum(if(dt >= date_sub('$do_date', 6),coupon_reduce_amount_1d, 0)) coupon_reduce_amount_7d,
    sum(if(dt >= date_sub('$do_date', 6),order_total_amount_1d, 0)) order_total_amount_7d,
    sum(order_count_1d) order_count_30d,
    sum(order_num_1d) order_num_30d,
    sum(order_original_amount_1d) order_original_amount_30d,
    sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_30d,
    sum(order_total_amount_1d) order_total_amount_30d
from ${APP}.dws_trade_user_sku_order_1d
where dt >= date_sub('$do_date',29)
group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;"

dws_trade_user_sku_order_refund_nd="
insert overwrite table ${APP}.dws_trade_user_sku_order_refund_nd partition (dt='$do_date')
select
    user_id,
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    sum(if(dt >= date_sub('$do_date',6),order_refund_count_1d,0)) order_refund_count_7d,
    sum(if(dt >= date_sub('$do_date',6),order_refund_num_1d,0)) order_refund_num_7d,
    sum(if(dt >= date_sub('$do_date',6),order_refund_amount_1d,0)) order_refund_amount_7d,
    sum(order_refund_count_1d) order_refund_count_30d,
    sum(order_refund_num_1d) order_refund_num_30d,
    sum(order_refund_amount_1d) order_refund_amount_30d
from ${APP}.dws_trade_user_sku_order_refund_1d
where dt >= date_sub('$do_date',29) and dt <= '$do_date'
group by user_id, sku_id, sku_name, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;"

dws_trade_user_order_nd="
insert overwrite table ${APP}.dws_trade_user_order_nd partition (dt='$do_date')
select
    user_id,
    sum(if(dt >= date_sub('$do_date',6),order_count_1d, 0)) order_count_7d,
    sum(if(dt >= date_sub('$do_date',6),order_num_1d, 0)) order_num_7d,
    sum(if(dt >= date_sub('$do_date',6),order_original_amount_1d, 0)) order_original_amount_7d,
    sum(if(dt >= date_sub('$do_date',6),activity_reduce_amount_1d, 0)) activity_reduce_amount_7d,
    sum(if(dt >= date_sub('$do_date',6),coupon_reduce_amount_1d, 0)) coupon_reduce_amount_7d,
    sum(if(dt >= date_sub('$do_date',6),order_original_amount_1d, 0)) order_total_amount_7d,
    sum(order_count_1d) order_count_30d,
    sum(order_num_1d) order_num_30d,
    sum(order_original_amount_1d) order_original_amount_30d,
    sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_30d,
    sum(order_total_amount_1d) order_total_amount_30d
from ${APP}.dws_trade_user_order_1d
where dt >= date_sub('$do_date',29) and dt <= '$do_date'
group by user_id;"

dws_trade_user_cart_add_nd="
insert overwrite table ${APP}.dws_trade_user_cart_add_nd partition (dt='$do_date')
select
    user_id,
    sum(if(dt >= date_sub('$do_date',6),cart_add_count_1d, 0)) cart_add_count_7d,
    sum(if(dt >= date_sub('$do_date',6),cart_add_num_1d, 0)) cart_add_num_7d,
    sum(cart_add_count_1d) cart_add_count_30d,
    sum(cart_add_num_1d) cart_add_num_30d
from ${APP}.dws_trade_user_cart_add_1d
where dt >= date_sub('$do_date',29) and dt <= '$do_date'
group by user_id;"

dws_trade_user_payment_nd="
insert overwrite table ${APP}.dws_trade_user_payment_nd partition (dt='$do_date')
select
    user_id,
    sum(if(dt >= date_sub('$do_date',6),payment_count_1d,0)) payment_count_7d,
    sum(if(dt >= date_sub('$do_date',6),payment_num_1d,0)) payment_num_7d,
    sum(if(dt >= date_sub('$do_date',6),payment_amount_1d,0)) payment_amount_7d,
    sum(payment_count_1d) payment_count_30d,
    sum(payment_num_1d) payment_num_30d,
    sum(payment_amount_1d) payment_amount_30d
from ${APP}.dws_trade_user_payment_1d
where dt>=date_sub('$do_date',29) and dt <= '$do_date'
group by user_id;"

dws_trade_province_order_nd="
insert overwrite table ${APP}.dws_trade_province_order_nd partition (dt='$do_date')
select
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    sum(if(dt >= date_sub('$do_date',6), order_count_1d, 0)) order_count_7d,
    sum(if(dt >= date_sub('$do_date',6), order_original_amount_1d, 0)) order_original_amount_7d,
    sum(if(dt >= date_sub('$do_date',6), activity_reduce_amount_1d, 0)) activity_reduce_amount_7d,
    sum(if(dt >= date_sub('$do_date',6), coupon_reduce_amount_1d, 0)) coupon_reduce_amount_7d,
    sum(if(dt >= date_sub('$do_date',6), order_total_amount_1d, 0)) order_total_amount_7d,
    sum(order_count_1d) order_count_30d,
    sum(order_original_amount_1d) order_original_amount_30d,
    sum(activity_reduce_amount_1d) activity_reduce_amount_30d,
    sum(coupon_reduce_amount_1d) coupon_reduce_amount_30d,
    sum(order_original_amount_1d) order_total_amount_30d
from ${APP}.dws_trade_province_order_1d
where dt >= date_sub('$do_date', 29) and dt <= '$do_date'
group by province_id, province_name, area_code, iso_code, iso_3166_2;"

dws_trade_coupon_order_nd="
insert overwrite table ${APP}.dws_trade_coupon_order_nd partition (dt='$do_date')
select
    coupon_id,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    coupon_rule,
    start_date,
    sum(split_original_amount) original_amount_30d,
    sum(split_coupon_amount) coupon_reduce_amount_30d
from (
         select
             coupon_id,
             split_original_amount,
             split_coupon_amount
         from ${APP}.dwd_trade_order_detail_inc
         where dt>= date_sub('$do_date', 29)
           and dt <= '$do_date'
           and coupon_id is not null
     ) od
right join
    (
        select
            id,
            coupon_name,
            coupon_type_code,
            coupon_type_name,
            benefit_rule coupon_rule,
            date_format(start_time,'yyyy-MM-dd') start_date
        from ${APP}.dim_coupon_full
        where dt='$do_date'
        and date_format(start_time,'yyyy-MM-dd') >= date_sub('$do_date',29)
    ) cou
on od.coupon_id = cou.id
group by coupon_id, coupon_name, coupon_type_code, coupon_type_name, coupon_rule, start_date;"

dws_trade_activity_order_nd="
insert overwrite table ${APP}.dws_trade_activity_order_nd partition (dt='$do_date')
select
    act.activity_id,
    activity_name,
    activity_type_code,
    activity_type_name,
    date_format(start_time, 'yyyy-MM-dd') start_date,
    sum(split_original_amount) original_amount_30d,
    sum(split_activity_amount) activity_reduce_amount_30d
from (
    select
        activity_id,
        activity_name,
        activity_type_code,
        activity_type_name,
        start_time
    from ${APP}.dim_activity_full
    where dt='$do_date'
       and date_format(start_time, 'yyyy-MM-dd') >= date_sub('$do_date', 29)
    group by activity_id, activity_name, activity_type_code, activity_type_name, start_time
     ) act
left join
    (
        select
            activity_id,
            split_original_amount,
            split_activity_amount
        from ${APP}.dwd_trade_order_detail_inc
        where dt >= date_sub('$do_date', 29) and dt <= '$do_date'
        and activity_id is not null
    ) od
on act.activity_id = od.activity_id
group by act.activity_id, activity_name, activity_type_code, activity_type_name, start_time;"

dws_trade_user_order_refund_nd="
insert overwrite table ${APP}.dws_trade_user_order_refund_nd partition (dt='$do_date')
select
    user_id,
    sum(if(dt >= date_sub('$do_date',6),order_refund_count_1d, 0)) order_refund_count_7d,
    sum(if(dt >= date_sub('$do_date',6),order_refund_num_1d, 0)) order_refund_num_7d,
    sum(if(dt >= date_sub('$do_date',6),order_refund_amount_1d, 0)) order_refund_amount_7d,
    sum(order_refund_count_1d) order_refund_count_30d,
    sum(order_refund_num_1d) order_refund_num_30d,
    sum(order_refund_amount_1d) order_refund_amount_30d
from ${APP}.dws_trade_user_order_refund_1d
where dt >= date_sub('$do_date', 29) and dt <= '$do_date'
group by user_id;"

dws_traffic_page_visitor_page_view_nd="
insert overwrite table ${APP}.dws_traffic_page_visitor_page_view_nd partition (dt='$do_date')
select
    mid_id,
    brand,
    model,
    operate_system,
    page_id,
    sum(if(dt >= date_sub('$do_date',6),during_time_1d,0)) during_time_7d,
    sum(if(dt >= date_sub('$do_date',6),view_count_1d,0)) view_count_7d,
    sum(during_time_1d) during_time_30d,
    sum(view_count_1d) view_count_30d
from ${APP}.dws_traffic_page_visitor_page_view_1d
where dt >= date_sub('$do_date', 29) and dt <= '$do_date'
group by mid_id, brand, model, operate_system, page_id;"

case $1 in
  "dws_trade_user_sku_order_nd")
  	hive -e "$dws_trade_user_sku_order_nd"
  ;;
  "dws_trade_user_sku_order_refund_nd")
  	hive -e "$dws_trade_user_sku_order_refund_nd"
  ;;
  "dws_trade_user_order_nd")
  	hive -e "$dws_trade_user_order_nd"
  ;;
  "dws_trade_user_cart_add_nd")
  	hive -e "$dws_trade_user_cart_add_nd"
  ;;
  "dws_trade_user_payment_nd")
  	hive -e "$dws_trade_user_payment_nd"
  ;;
  "dws_trade_province_order_nd")
  	hive -e "$dws_trade_province_order_nd"
  ;;
  "dws_trade_coupon_order_nd")
  	hive -e "$dws_trade_coupon_order_nd"
  ;;
  "dws_trade_activity_order_nd")
  	hive -e "$dws_trade_activity_order_nd"
  ;;
  "dws_trade_user_order_refund_nd")
  	hive -e "$dws_trade_user_order_refund_nd"
  ;;
  "dws_traffic_page_visitor_page_view_nd")
  	hive -e "$dws_traffic_page_visitor_page_view_nd"
  ;;
  "all")
  	hive -e "$dws_trade_user_sku_order_nd$dws_trade_user_sku_order_refund_nd$dws_trade_user_order_nd$dws_trade_user_cart_add_nd$dws_trade_user_payment_nd$dws_trade_province_order_nd$dws_trade_coupon_order_nd$dws_trade_activity_order_nd$dws_trade_user_order_refund_nd$dws_traffic_page_visitor_page_view_nd"
esac