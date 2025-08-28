package com.app.ads;

import com.app.bean.AdsUserBehaviorStats;
import com.stream.common.utils.ClickHouseUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Objects;

public class AdsUserBehaviorStatsJob {
    public static void main(String[] args) throws Exception {
        // 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 动态 groupId 防止缓存
        String groupId = "ads_user_behavior_stats_" + System.currentTimeMillis();

        // 3. 创建 Kafka Source 表
        tableEnv.executeSql(
                "CREATE TABLE dws_user_behavior_enriched_gongdan ( " +
                        "  user_id STRING, " +
                        "  event_type STRING, " +
                        "  session_id STRING, " +
                        "  duration DOUBLE, " +
                        "  proctime AS PROCTIME() " +
                        ") WITH ( " +
                        "  'connector' = 'kafka', " +
                        "  'topic' = 'dws_user_behavior_enriched_gongdan', " +
                        "  'properties.bootstrap.servers' = 'cdh01:9092', " +
                        "  'properties.group.id' = '" + groupId + "', " +
                        "  'scan.startup.mode' = 'earliest-offset', " +
                        "  'format' = 'json' " +
                        ")"
        );

        // 4. 聚合 SQL 查询（修复 NULL 报错）
        Table resultTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "  user_id, \n" +
                        "  SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS pv, \n" +
                        "  SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS click, \n" +
                        "  SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS cart, \n" +
                        "  SUM(CASE WHEN event_type = 'pay' THEN 1 ELSE 0 END) AS pay, \n" +
                        "  COUNT(DISTINCT session_id) AS sessionCt, \n" +
                        "  COUNT(*) - COUNT(DISTINCT session_id) AS bounceCt, \n" +
                        "  ROUND(AVG(duration), 2) AS avgStayDuration \n" +
                        "FROM TABLE( \n" +
                        "  TUMBLE(TABLE dws_user_behavior_enriched_gongdan, DESCRIPTOR(proctime), INTERVAL '1' MINUTE) \n" +
                        ") \n" +
                        "WHERE user_id IS NOT NULL \n" +
                        "GROUP BY user_id"
        );

        // 5. 转换为实体类
        SingleOutputStreamOperator<AdsUserBehaviorStats> resultStream = tableEnv.toDataStream(resultTable, Row.class)
                .map(row -> {
                    AdsUserBehaviorStats stats = new AdsUserBehaviorStats();
                    stats.setUserId((String) row.getField(0));
                    stats.setPv(((Number) Objects.requireNonNull(row.getField(1))).longValue());
                    stats.setClick(((Number) Objects.requireNonNull(row.getField(2))).longValue());
                    stats.setCart(((Number) Objects.requireNonNull(row.getField(3))).longValue());
                    stats.setPay(((Number) Objects.requireNonNull(row.getField(4))).longValue());
                    stats.setSessionCt(((Number) Objects.requireNonNull(row.getField(5))).longValue());
                    stats.setBounceCt(((Number) Objects.requireNonNull(row.getField(6))).longValue());
                    stats.setAvgStayDuration((Double) row.getField(7));
                    return stats;
                }, Types.POJO(AdsUserBehaviorStats.class));

        // 6. 调试输出
        resultStream.print("user_behavior");

        // 7. 写入 ClickHouse
        resultStream.addSink(ClickHouseUtil.getSink(
                "INSERT INTO ads_user_behavior_stats " +
                        "(user_id, pv, click, cart, pay, session_ct, bounce_ct, avg_stay_duration) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        ));

        // 8. 启动作业
        env.execute();
    }
}
