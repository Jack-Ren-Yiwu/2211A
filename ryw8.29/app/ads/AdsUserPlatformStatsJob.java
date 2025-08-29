package com.app.ads;

import com.app.bean.AdsUserPlatformStats;
import com.stream.common.utils.ClickHouseUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Objects;

public class AdsUserPlatformStatsJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 动态 groupId
        String groupId = "ads_user_platform_stats_debug_" + System.currentTimeMillis();

        // 3. 创建 Kafka 源表（根据你的实际宽表结构调整字段）
        tableEnv.executeSql(
                "CREATE TABLE dws_order_enriched_gongdan ( " +
                        "  user_id STRING, " +
                        "  user_id_channel STRING, " +
                        "  order_id STRING, " +
                        "  order_amount DOUBLE, " +
                        "  proctime AS PROCTIME() " +
                        ") WITH ( " +
                        "  'connector' = 'kafka', " +
                        "  'topic' = 'dws_order_enriched_gongdan', " +
                        "  'properties.bootstrap.servers' = 'cdh01:9092', " +
                        "  'properties.group.id' = '" + groupId + "', " +
                        "  'scan.startup.mode' = 'earliest-offset', " +
                        "  'format' = 'json' " +
                        ")"
        );
       // tableEnv.executeSql("select * from dws_order_enriched_gongdan").print();
        // 4. 聚合查询：统计每分钟每个平台的用户数、订单数、支付总金额、平均客单价
        Table resultTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  user_id_channel, " +
                        "  COUNT(DISTINCT user_id) AS user_cnt, " +
                        "  COUNT(DISTINCT order_id) AS order_cnt, " +
                        "  SUM(order_amount) AS pay_amount, " +
                        "  IF(COUNT(DISTINCT order_id) > 0, SUM(order_amount) / COUNT(DISTINCT order_id), 0.0) AS avg_order_amount " +
                        "FROM TABLE( " +
                        "  TUMBLE(TABLE dws_order_enriched_gongdan, DESCRIPTOR(proctime), INTERVAL '1' MINUTE) " +
                        ") " +
                        "WHERE user_id_channel IS NOT NULL AND user_id IS NOT NULL " +
                        "GROUP BY user_id_channel, window_start, window_end"
        );

        // 5. 转为实体类流
        SingleOutputStreamOperator<AdsUserPlatformStats> resultStream =
                tableEnv.toDataStream(resultTable, Row.class)
                        .map(row -> {
                            AdsUserPlatformStats stats = new AdsUserPlatformStats();
                            stats.setPlatform((String) row.getField(0));
                            stats.setUserCnt(((Number) Objects.requireNonNull(row.getField(1))).longValue());
                            stats.setOrderCnt(((Number) Objects.requireNonNull(row.getField(2))).longValue());
                            stats.setPayAmount((Double) row.getField(3));
                            stats.setAvgOrderAmount((Double) row.getField(4));
                            return stats;
                        }, Types.POJO(AdsUserPlatformStats.class));

        // 6. 调试输出
        resultStream.print("platform_stats");

        // 7. 写入 ClickHouse
        resultStream.addSink(ClickHouseUtil.getSink(
                "INSERT INTO ads_user_platform_stats " +
                        "(platform, user_cnt, order_cnt, pay_amount, avg_order_amount) " +
                        "VALUES (?, ?, ?, ?, ?)"
        ));

        // 8. 启动任务
        env.execute("AdsUserPlatformStatsJob");
    }
}
