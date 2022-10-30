#!/bin/bash

if [ $# -lt 1 ]
then
  echo "输入的参数个数有误!"
  echo "参数1：必选 要转载的表,装载全部请输入 all"
  echo "参数2：可选 装载数据的日期，不选则取当前日期的前一日"
  exit
fi

APP=gmall

if [ -n "$2" ]
then
  do_date=$2
else
  do_date=`date -d '-1 day' +%F`
fi

ads_traffic_stats_by_channel="
insert overwrite table ${APP}.ads_traffic_stats_by_channel
select * from ${APP}.ads_traffic_stats_by_channel
union
select
    '$do_date',
    recent_days,
    channel,
    cast(count(distinct mid_id) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1, 1, 0))/count(*) as decimal(16, 2)) bounce_rate
from ${APP}.dws_traffic_session_page_view_1d lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt >= date_sub('$do_date', recent_days-1)
group by recent_days, channel;"

ads_page_path="
insert overwrite table ${APP}.ads_page_path
select * from ${APP}.ads_page_path
union
select
    '$do_date',
    recent_days,
    source,
    nvl(target, null),
    count(*) path_count
from
(
    select
        recent_days,
        concat('step-', rn, ':', page_id) source,
        concat('step-', rn+1, ':', next_page_id) target
    from
        (
            select
                recent_days,
                page_id,
                lead(page_id, 1, null) over(partition by session_id, recent_days order by view_time) next_page_id,
                row_number() over (partition by session_id, recent_days order by view_time) rn
            from ${APP}.dwd_traffic_page_view_inc lateral view explode(array(1,7,30)) tmp as recent_days
            where dt >= date_sub('$do_date', recent_days-1)
        ) t1
) t2
group by recent_days,source, target;"

ads_user_change="
insert overwrite table ${APP}.ads_user_change
select * from ${APP}.ads_user_change
union
select
    back.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '$do_date' dt,
        count(*) user_churn_count
    from ${APP}.dws_user_user_login_td
    where dt='$do_date'
      and login_date_last = date_sub('$do_date', 7)
) churn
join
(
    select
        '$do_date' dt,
        count(*) user_back_count
    from
        (
            select
                user_id,
                login_date_last
            from ${APP}.dws_user_user_login_td
            where dt='$do_date'
        ) t1
            join
        (
            select
                user_id,
                login_date_last login_date_pre
            from ${APP}.dws_user_user_login_td
            where dt=date_sub('$do_date', 1)
        ) t2
        on t1.user_id=t2.user_id
    where datediff(login_date_pre,login_date_last) >= 8
) back
on churn.dt = back.dt;"

ads_user_retention="
insert overwrite table ${APP}.ads_user_retention
select * from ${APP}.ads_user_retention
union
select
    '$do_date' dt,
    date_id create_date,
    datediff('$do_date',date_id) retention_day,
    cast(sum(if(login_date_last='$do_date',1,0)) as bigint) retention_count,
    cast(count(*) as bigint) new_user_count,
    cast(sum(if(login_date_last='$do_date',1,0))/count(*) as decimal(16,2)) retention_rate
from
    (
        select
            user_id,
            date_id
        from ${APP}.dwd_user_register_inc
        where dt >= date_sub('$do_date', 7)
        and dt < '$do_date'
    ) t1
join
    (
        select
            user_id,
            login_date_last
        from ${APP}.dws_user_user_login_td
        where dt='$do_date'
    ) t2
on t1.user_id=t2.user_id
group by date_id;"

ads_user_stats="
insert overwrite table ${APP}.ads_user_stats
select * from ${APP}.ads_user_stats
union
select
    '$do_date',
    new.recent_days,
    new_user_count,
    active_user_count
from
(
    select
        recent_days,
        sum(if(date_id >= date_sub('$do_date',recent_days-1), 1, 0)) new_user_count
    from ${APP}.dwd_user_register_inc lateral view explode(array(1, 7, 30)) tmp as recent_days
    group by recent_days
) new
join
(
    select
       recent_days,
       sum(if(login_date_last >= date_sub('$do_date', recent_days-1), 1, 0)) active_user_count
    from ${APP}.dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
    where dt = '$do_date'
    group by recent_days
) active
on new.recent_days=active.recent_days;"

ads_user_action="insert overwrite table ${APP}.ads_user_action
                 select * from ${APP}.ads_user_action
                 union
                 select
                     '$do_date',
                     od.recent_days,
                     home_count,
                     good_detail_count,
                     cart_count,
                     order_count,
                     payment_count
                 from
                     (
                         select
                             1 recent_days,
                             sum(if(page_id='home',1,0)) home_count,
                             sum(if(page_id='good_detail',1,0)) good_detail_count
                         from ${APP}.dws_traffic_page_visitor_page_view_1d
                         where dt='$do_date' and page_id in ('home','good_detail')
                         union
                         select
                             recent_days,
                             sum(if(page_id='home' and view_count>0, 1, 0)) home_count,
                             sum(if(page_id='good_detail' and view_count>0,1,0)) good_detail_count
                         from
                             (
                                 select
                                     recent_days,
                                     page_id,
                                     case recent_days
                                         when 7 then view_count_7d
                                         when 30 then view_count_30d
                                         end view_count
                                 from ${APP}.dws_traffic_page_visitor_page_view_nd lateral view explode(array(7,30)) tmp as recent_days
                                 where dt='$do_date' and page_id in ('home','good_detail')
                             ) t1
                         group by recent_days
                     ) page
                 join
                     (
                         select
                             1 recent_days,
                             count(*) cart_count
                         from ${APP}.dws_trade_user_cart_add_1d
                         where dt='$do_date'
                         union
                         select
                             recent_days,
                             sum(if(cart_count>0,1,0)) cart_count
                         from (
                                  select
                                      recent_days,
                                      case recent_days
                                          when 7 then cart_add_count_7d
                                          when 30 then cart_add_count_30d
                                          end cart_count
                                  from ${APP}.dws_trade_user_cart_add_nd lateral view explode(array(7,30)) tmp as recent_days
                                  where dt='$do_date'
                              ) t1
                         group by recent_days
                     ) cart
                 on page.recent_days=cart.recent_days
                 join
                     (
                         select
                             1 recent_days,
                             count(*) order_count
                         from ${APP}.dws_trade_user_order_1d
                         where dt='$do_date'
                         union
                         select
                             recent_days,
                             sum(if(order_count>0 , 1, 0)) order_count
                         from
                             (
                                 select
                                     recent_days,
                                     case recent_days
                                         when 7 then order_count_7d
                                         when 30 then order_count_30d
                                         end order_count
                                 from ${APP}.dws_trade_user_order_nd lateral view explode(array(7,30)) tmp as recent_days
                                 where dt='$do_date'
                             ) t1
                         group by recent_days
                     ) od
                 on page.recent_days=od.recent_days
                 join
                     (
                         select
                             1 recent_days,
                             count(*) payment_count
                         from ${APP}.dws_trade_user_payment_1d
                         where dt='$do_date'
                         union
                         select
                             recent_days,
                             sum(if(payment_count>0, 1, 0)) payment_count
                         from
                             (
                                 select
                                     recent_days,
                                     case recent_days
                                         when 7 then payment_count_7d
                                         when 30 then payment_count_30d
                                         end payment_count
                                 from ${APP}.dws_trade_user_payment_nd lateral view explode(array(7,30)) tmp as recent_days
                                 where dt='$do_date'
                             ) t1
                         group by recent_days
                     ) payment
                 on page.recent_days=payment.recent_days;"

ads_new_buyer_stats="
insert overwrite table ${APP}.ads_new_buyer_stats
select * from ${APP}.ads_new_buyer_stats
union
select
    '$do_date',
    np.recent_days,
    new_order_user_count,
    new_payment_user_count
from (
         select
             recent_days,
             sum(if(order_date_first >= date_sub('$do_date', recent_days-1), 1, 0)) new_order_user_count
         from ${APP}.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
         where dt = '$do_date'
         group by recent_days
     ) no
join
    (
        select
            recent_days,
            sum(if(payment_date_first>= date_sub('$do_date', recent_days-1), 1, 0)) new_payment_user_count
        from ${APP}.dws_trade_user_payment_td lateral view explode(array(1,7,30)) tmp as recent_days
        where dt ='$do_date'
        group by recent_days
    ) np
on no.recent_days=np.recent_days;"

ads_repeat_purchase_by_tm="
insert overwrite table ${APP}.ads_repeat_purchase_by_tm
select * from ${APP}.ads_repeat_purchase_by_tm
union
select
    '$do_date',
    recent_days,
    tm_id,
    tm_name,
    cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2)) order_repeat_rate
from
    (
        select
            recent_days,
            tm_id,
            tm_name,
            sum(order_count) order_count
        from
            (
                select
                    recent_days,
                    user_id,
                    tm_id,
                    tm_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                    end order_count
                from ${APP}.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
                where dt='$do_date'
            ) t1
        group by recent_days,user_id,tm_id,tm_name
    ) t2
group by recent_days, tm_id, tm_name;"

ads_trade_stats_by_tm="
insert overwrite table ${APP}.ads_trade_stats_by_tm
select * from ${APP}.ads_trade_stats_by_tm
union
select
    '$do_date',
    nvl(od.recent_days,refund.recent_days),
    nvl(od.tm_id,refund.tm_id),
    nvl(od.tm_name,refund.tm_name),
    nvl(order_count, 0),
    nvl(order_user_count, 0),
    nvl(order_refund_count, 0),
    nvl(order_refund_user_count, 0)
from
    (
        select
            1 recent_days,
            tm_id,
            tm_name,
            sum(order_count_1d) order_count,
            count(distinct user_id) order_user_count
        from ${APP}.dws_trade_user_sku_order_1d
        where dt = '$do_date'
        group by tm_id, tm_name
        union
        select
            recent_days,
            tm_id,
            tm_name,
            sum(order_count) order_count,
            count(distinct(if(order_count>0, user_id, null))) order_user_count
        from
          (
              select
                  recent_days,
                  user_id,
                  tm_id,
                  tm_name,
                  case recent_days
                      when 7 then order_count_7d
                      when 30 then order_count_30d
                  end order_count
              from ${APP}.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
              where dt='$do_date'
          ) t1
        group by recent_days, tm_id, tm_name
    ) od
full outer join
    (
        select
            1 recent_days,
            tm_id,
            tm_name,
            sum(order_refund_count_1d) order_refund_count,
            count(distinct user_id) order_refund_user_count
        from ${APP}.dws_trade_user_sku_order_refund_1d
        where dt='$do_date'
        group by tm_id, tm_name
        union
        select
            recent_days,
            tm_id,
            tm_name,
            sum(order_refund_count) order_refund_count,
            count(if(order_refund_count>0, user_id, null)) order_refund_user_count
        from
            (
                select
                    recent_days,
                    user_id,
                    tm_id,
                    tm_name,
                    case recent_days
                        when 7 then order_refund_count_7d
                        when 30 then order_refund_count_30d
                    end order_refund_count
                from ${APP}.dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
                where dt='$do_date'
            ) t1
        group by recent_days, tm_id, tm_name
    ) refund
on od.recent_days=refund.recent_days
and od.tm_id=refund.tm_id
and od.tm_name=refund.tm_name;"

ads_trade_stats_by_cate="
insert overwrite table ${APP}.ads_trade_stats_by_cate
select * from ${APP}.ads_trade_stats_by_cate
union
select
    '$do_date',
    nvl(ord.recent_days, refund.recent_days),
    nvl(ord.category1_id, refund.category1_id),
    nvl(ord.category1_name, refund.category1_name),
    nvl(ord.category2_id, refund.category2_id),
    nvl(ord.category2_name, refund.category2_name),
    nvl(ord.category3_id, refund.category3_id),
    nvl(ord.category3_name, refund.category3_name),
    nvl(order_count, 0),
    nvl(order_user_count,0),
    nvl(order_refund_count, 0),
    nvl(order_refund_user_count ,0)
from
    (
        select
            1 recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_count_1d) order_count,
            count(distinct user_id) order_user_count
        from ${APP}.dws_trade_user_sku_order_1d
        where dt='$do_date'
        group by category1_id, 1, category1_name, category2_id, category2_name, category3_id, category3_name
        union
        select
            recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_count) order_count,
            count(distinct(if(order_count>0, user_id, null))) order_user_count
        from
            (
                select
                    recent_days,
                    user_id,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from ${APP}.dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                where dt='$do_date'
            ) t1
        group by recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
    ) ord
full outer join
    (
        select
            1 recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_refund_count_1d) order_refund_count,
            count(distinct user_id) order_refund_user_count
        from ${APP}.dws_trade_user_sku_order_refund_1d
        where dt='$do_date'
        group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
        union
        select
            recent_days,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sum(order_refund_count) order_refund_count,
            count(distinct(if(order_refund_count>0, user_id, null))) order_refund_user_count
        from
            (
                select
                    recent_days,
                    user_id,
                    category1_id,
                    category1_name,
                    category2_id,
                    category2_name,
                    category3_id,
                    category3_name,
                    case recent_days
                        when 7 then order_refund_count_7d
                        when 30 then order_refund_count_30d
                        end order_refund_count
                from ${APP}.dws_trade_user_sku_order_refund_nd lateral view explode(array(7 ,30)) tmp as recent_days
                where dt='$do_date'
            ) t1
        group by recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name
    ) refund
on ord.recent_days=refund.recent_days
and ord.category1_id=refund.category1_id
and ord.category1_name=refund.category1_name
and ord.category2_id=refund.category2_id
and ord.category2_name=refund.category2_name
and ord.category3_id=refund.category3_id
and ord.category3_name=refund.category3_name;"

ads_sku_cart_num_top3_by_cate="
insert overwrite table ${APP}.ads_sku_cart_num_top3_by_cate
select * from ${APP}.ads_sku_cart_num_top3_by_cate
union
select
    '$do_date',
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
from
    (
        select
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name,
            sku_id,
            sku_name,
            cart_num,
            dense_rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
        from
            (
                select
                    sku_id,
                    sum(sku_num) cart_num
                from ${APP}.dwd_trade_cart_full
                where dt='$do_date'
                group by sku_id
            ) sku
                left join
                (
                    select
                        id,
                        sku_name,
                        category1_id,
                        category1_name,
                        category2_id,
                        category2_name,
                        category3_id,
                        category3_name
                    from ${APP}.dim_sku_full
                ) cate
            on sku.sku_id=cate.id
            ) t1
where rk <= 3;"

ads_trade_stats="
insert overwrite table ${APP}.ads_trade_stats
select * from ${APP}.ads_trade_stats
union
select
    '$do_date',
    nvl(odr.recent_days, refund.recent_days),
    nvl(odr.order_total_amount, 0),
    nvl(odr.order_count, 0),
    nvl(odr.order_user_count, 0),
    nvl(refund.order_refund_count, 0),
    nvl(refund.order_refund_user_count, 0)
from
    (
        select
            1 recent_days,
            sum(order_total_amount_1d) order_total_amount,
            sum(order_count_1d) order_count,
            count(*) order_user_count
        from ${APP}.dws_trade_user_order_1d
        where dt='$do_date'
        union
        select
            recent_days,
            sum(nvl(order_total_amount, 0)) order_total_amount,
            sum(nvl(order_count, 0)) order_count,
            sum(if(order_total_amount>0, 1, 0)) order_user_count
        from
            (
                select
                    recent_days,
                    case recent_days
                        when 7 then order_total_amount_7d
                        when 30 then order_total_amount_30d
                        end order_total_amount,
                    case recent_days
                        when 7 then order_count_7d
                        when 30 then order_count_30d
                        end order_count
                from ${APP}.dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days
                where dt='$do_date'
            ) t1
        group by recent_days
    ) odr
full outer join
    (
        select
            1 recent_days,
            sum(order_refund_count_1d) order_refund_count,
            count(*) order_refund_user_count
        from ${APP}.dws_trade_user_order_refund_1d
        where dt='$do_date'
        union
        select
            recent_days,
            sum(order_refund_count) order_refund_count,
            sum(if(order_refund_count>0,1,0)) order_refund_user_count
        from
            (
                select
                    recent_days,
                    case recent_days
                        when 7 then order_refund_count_7d
                        when 30 then order_refund_count_30d
                        end order_refund_count
                from ${APP}.dws_trade_user_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days
                where dt='$do_date'
            ) t1
        group by recent_days
    ) refund
on odr.recent_days=refund.recent_days;"

ads_order_by_province="
insert overwrite table ${APP}.ads_order_by_province
select * from ${APP}.ads_order_by_province
union
select
    '$do_date',
    1 recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2 iso_code_3166_2,
    order_count_1d order_count,
    order_total_amount_1d order_total_amount
from ${APP}.dws_trade_province_order_1d
where dt='$do_date'
union
select
    '$do_date',
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    case recent_days
        when 7 then order_count_7d
        when 30 then order_count_30d
        end order_count,
    case recent_days
        when 7 then order_total_amount_7d
        when 30 then order_total_amount_30d
        end order_total_amount
from ${APP}.dws_trade_province_order_nd lateral view explode(array(7, 30)) tmp as recent_days
where dt='$do_date';"

ads_coupon_stats="
insert overwrite table ${APP}.ads_coupon_stats
select * from ${APP}.ads_coupon_stats
union
select
    '$do_date',
    coupon_id,
    coupon_name,
    start_date,
    coupon_rule,
    cast(coupon_reduce_amount_30d/original_amount_30d as decimal(16,2))
from ${APP}.dws_trade_coupon_order_nd
where dt='$do_date';"

ads_activity_stats="
insert overwrite table ${APP}.ads_activity_stats
select * from ${APP}.ads_activity_stats
union
select
    '$do_date',
    activity_id,
    activity_name,
    start_date,
    cast(activity_reduce_amount_30d/original_amount_30d as decimal(16, 2))
from ${APP}.dws_trade_activity_order_nd
where dt='$do_date';"

case $1 in
"ads_traffic_stats_by_channel")
    hive -e "$ads_traffic_stats_by_channel"
;;
"ads_page_path")
    hive -e "$ads_page_path"
;;
"ads_user_change")
    hive -e "$ads_user_change"
;;
"ads_user_retention")
    hive -e "$ads_user_retention"
;;
"ads_user_stats")
    hive -e "$ads_user_stats"
;;
"ads_user_action")
    hive -e "$ads_user_action"
;;
"ads_new_buyer_stats")
    hive -e "$ads_new_buyer_stats"
;;
"ads_repeat_purchase_by_tm")
    hive -e "$ads_repeat_purchase_by_tm"
;;
"ads_trade_stats_by_tm")
    hive -e "$ads_trade_stats_by_tm"
;;
"ads_trade_stats_by_cate")
    hive -e "$ads_trade_stats_by_cate"
;;
"ads_sku_cart_num_top3_by_cate")
    hive -e "$ads_sku_cart_num_top3_by_cate"
;;
"ads_trade_stats")
    hive -e "$ads_trade_stats"
;;
"ads_order_by_province")
    hive -e "$ads_order_by_province"
;;
"ads_coupon_stats")
    hive -e "$ads_coupon_stats"
;;
"ads_activity_stats")
    hive -e "$ads_activity_stats"
;;
"all")
    hive -e "$ads_traffic_stats_by_channel$ads_page_path$ads_user_change$ads_user_retention$ads_user_stats$ads_user_action$ads_new_buyer_stats$ads_repeat_purchase_by_tm$ads_trade_stats_by_tm$ads_trade_stats_by_cate$ads_sku_cart_num_top3_by_cate$ads_trade_stats$ads_order_by_province$ads_coupon_stats$ads_activity_stats"
;;
esac