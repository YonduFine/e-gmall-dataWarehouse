#! /bin/bash
if [ $# -lt 1  ]
then
	echo "参数输入有误，请输入start | stop !!!"
	exit
fi

case $1 in
"start")
	for host in bigdata101 bigdata102
	do
		ssh $host "cd /opt/module/applog; java -jar gmall2020-mock-log-2021-10-10.jar >/dev/null 2>&1 &"
    echo "------------> $host 日志生成脚本启动... <------------"
	done
;;
"stop")
	for host in bigdata101 bigdata102
	do
		ssh $host "ps -ef | grep -v 'grep' | grep -i gmall2020 | awk '{print \$2}' | xargs -n1 kill -9"
    echo "------------> $host 日志生成脚本停止... <------------"
	done
;;
*)
	echo "参数有误,请输入start|stop"
	exit
;;
esac;
