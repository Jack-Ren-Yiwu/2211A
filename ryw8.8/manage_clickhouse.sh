#!/bin/bash

# 三台 ClickHouse 节点主机名
NODES=("cdh01" "cdh02" "cdh03")

# 集群配置文件内容（<yandex> 根节点）
CLUSTER_CONFIG='<?xml version="1.0"?>
<yandex>
    <remote_servers>
        <my_cluster>
            <shard>
                <replica>
                    <host>cdh01</host>
                    <port>19100</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>cdh02</host>
                    <port>19100</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>cdh03</host>
                    <port>19100</port>
                </replica>
            </shard>
        </my_cluster>
    </remote_servers>
</yandex>'

upload_config() {
    echo "=== 上传 ClickHouse 集群配置 ==="
    for node in "${NODES[@]}"; do
        echo "上传到 $node"
        ssh $node "sudo mkdir -p /etc/clickhouse-server/config.d"
        echo "$CLUSTER_CONFIG" | ssh $node "sudo tee /etc/clickhouse-server/config.d/cluster.xml > /dev/null"
    done
}

start_cluster() {
    echo "=== 启动 ClickHouse 服务 ==="
    for node in "${NODES[@]}"; do
        echo "启动 $node 的 ClickHouse"
        ssh $node "sudo systemctl start clickhouse-server"
    done
    echo "启动完成！"
}

stop_cluster() {
    echo "=== 停止 ClickHouse 服务 ==="
    for node in "${NODES[@]}"; do
        echo "停止 $node 的 ClickHouse"
        ssh $node "sudo systemctl stop clickhouse-server"
    done
    echo "停止完成！"
}

case "$1" in
    start)
        upload_config
        start_cluster
        ;;
    stop)
        stop_cluster
        ;;
    restart)
        stop_cluster
        upload_config
        start_cluster
        ;;
    *)
        echo "用法: $0 {start|stop|restart}"
        ;;
esac
