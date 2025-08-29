package com.app.ads;

import com.app.bean.AdsUserBehaviorStats;
import com.stream.common.utils.ClickHouseUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.RowKind;

import java.util.Objects;

public class AdsUserBehaviorStatsJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 动态 groupId
        String groupId = "ads_user_behavior_stats_" + System.currentTimeMillis();

        // 3. 注册 Kafka 表（行为宽表）
        tableEnv.executeSql(
                "CREATE TABLE dws_user_behavior_enriched_gongdan ( " +
                        "  user_id STRING, " +
                        "  behavior_type STRING, " +
                        "  session_id STRING, " +
                        "  behavior_time DOUBLE, " +
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

        // 4. 会话视图：每个 user + session 的停留时长和行为数
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW session_stats AS " +
                        "SELECT " +
                        "  user_id, " +
                        "  session_id, " +
                        "  COUNT(*) AS behavior_count, " +
                        "  MAX(behavior_time) - MIN(behavior_time) AS duration_ms " +
                        "FROM dws_user_behavior_enriched_gongdan " +
                        "WHERE user_id IS NOT NULL AND session_id IS NOT NULL " +
                        "GROUP BY user_id, session_id"
        );

        // 5. 最终聚合结果
        Table resultTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  u.user_id, " +
                        "  SUM(CASE WHEN behavior_type = 'view' THEN 1 ELSE 0 END) AS pv, " +
                        "  SUM(CASE WHEN behavior_type = 'click' THEN 1 ELSE 0 END) AS click, " +
                        "  SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart, " +
                        "  SUM(CASE WHEN behavior_type = 'pay' THEN 1 ELSE 0 END) AS pay, " +
                        "  COUNT(DISTINCT u.session_id) AS sessionCt, " +
                        "  SUM(CASE WHEN s.behavior_count = 1 THEN 1 ELSE 0 END) AS bounceCt, " +
                        "  ROUND(AVG(s.duration_ms), 2) AS avgStayDuration " +
                        "FROM dws_user_behavior_enriched_gongdan u " +
                        "JOIN session_stats s " +
                        "ON u.user_id = s.user_id AND u.session_id = s.session_id " +
                        "WHERE u.user_id IS NOT NULL " +
                        "GROUP BY u.user_id"
        );

        // 6. 转换为 POJO
        SingleOutputStreamOperator<AdsUserBehaviorStats> resultStream = tableEnv.toChangelogStream(resultTable)
                .filter(row -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER)
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
                }, Types.POJO(AdsUserBehaviorStats.class))
                .filter(stats ->
                        stats.getUserId() != null &&
                                stats.getPv() != null &&
                                stats.getClick() != null &&
                                stats.getCart() != null &&
                                stats.getPay() != null &&
                                stats.getSessionCt() != null &&
                                stats.getBounceCt() != null &&
                                stats.getAvgStayDuration() != null
                );

        // 7. 控制台输出
        resultStream.print("user_behavior");

        // 8. ClickHouse Sink（如需写入开启以下代码）

        resultStream.addSink(ClickHouseUtil.getSink(
                "INSERT INTO ads_user_behavior_stats " +
                        "(user_id, pv, click, cart, pay, session_ct, bounce_ct) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)"
        ));


        // 9. 启动作业
        env.execute("AdsUserBehaviorStatsJob");
    }
}
