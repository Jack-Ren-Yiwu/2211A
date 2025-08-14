#!/bin/bash
case $1 in
    "start")
        for host in cdh01 cdh02 cdh03 ; do
            echo "========== 在 $host 上启动 fe  ========="
            ssh $host "source /etc/profile; /opt/module/doris/apache-doris-2.1.10-bin-x64/fe/bin/start_fe.sh --daemon"
        done
        for host in cdh01 cdh02 cdh03 ; do
            echo "========== 在 $host 上启动 be  ========="
            ssh $host "source /etc/profile; /opt/module/doris/apache-doris-2.1.10-bin-x64/be/bin/start_be.sh --daemon"
        done
       ;;
    "stop")
        for host in cdh01 cdh02 cdh03 ; do
            echo "========== 在 $host 上停止 fe  ========="
            ssh $host "source /etc/profile; /opt/module/doris/apache-doris-2.1.10-bin-x64/fe/bin/stop_fe.sh"
        done
        for host in cdh01 cdh02 cdh03 ; do
            echo "========== 在 $host 上停止 be  ========="
            ssh $host "source /etc/profile; /opt/module/doris/apache-doris-2.1.10-bin-x64/be/bin/stop_be.sh"
        done
       ;;
    *)
        echo "你启动的姿势不对"
        echo "  start   启动doris集群"
        echo "  stop    停止doris集群"
       ;;
esac

