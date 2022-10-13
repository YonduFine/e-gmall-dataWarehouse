#! /bin/bash
if [ $# -lt 1 ]
then
    echo "参数不能为空! 请输入start | stop"
    exit 
fi

flumeconf=kafka_to_hdfs_log.conf
host=bigdata103

case $1 in
"start")
    ssh $host "nohup /opt/module/flume-1.9.0/bin/flume-ng agent -n a1 -c /conf -f /opt/module/flume-1.9.0/jobs/gmall/$flumeconf >/dev/null 2>&1 &"
    echo "$host 下游日志采集服务启动"
;;
"stop")
    ssh $host "ps -ef | grep -v 'grep' | grep -i $flumeconf | awk '{print \$2}' | xargs -n1 kill -9"
    echo "$host 下游日志采集服务已停止"
;;
*)
    echo "输入的参数有误! 请输入 start | stop"
;;
esac
