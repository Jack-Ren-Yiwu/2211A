package com.app.ads;

import com.app.bean.AdsUserFunnelConversion;
import com.stream.common.utils.ClickHouseUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Objects;

public class AdsUserFunnelConversionJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 动态 groupId
        String groupId = "ads_user_funnel_conversion_debug_" + System.currentTimeMillis();

        // 3. 注册 Kafka Source 表
        tableEnv.executeSql(
                "CREATE TABLE dws_user_behavior_enriched_gongdan ( " +
                        "  user_id STRING, " +
                        "  behavior_type STRING, " +
                        "  session_id STRING, " +
                        "  behavior_time BIGINT, " +
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

        // 4. 聚合 SQL：漏斗各阶段行为统计 + 转化率
        Table funnelTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  user_id, " +
                        "  SUM(CASE WHEN behavior_type = 'view' THEN 1 ELSE 0 END) AS view_cnt, " +
                        "  SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart_cnt, " +
                        "  SUM(CASE WHEN behavior_type = 'pay' THEN 1 ELSE 0 END) AS pay_cnt, " +
                        "  IF(SUM(CASE WHEN behavior_type = 'view' THEN 1 ELSE 0 END) = 0, 0.0, " +
                        "     SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) * 1.0 / " +
                        "     SUM(CASE WHEN behavior_type = 'view' THEN 1 ELSE 0 END)) AS view_to_cart_rate, " +
                        "  IF(SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) = 0, 0.0, " +
                        "     SUM(CASE WHEN behavior_type = 'pay' THEN 1 ELSE 0 END) * 1.0 / " +
                        "     SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END)) AS cart_to_pay_rate, " +
                        "  IF(SUM(CASE WHEN behavior_type = 'view' THEN 1 ELSE 0 END) = 0, 0.0, " +
                        "     SUM(CASE WHEN behavior_type = 'pay' THEN 1 ELSE 0 END) * 1.0 / " +
                        "     SUM(CASE WHEN behavior_type = 'view' THEN 1 ELSE 0 END)) AS view_to_pay_rate " +
                        "FROM dws_user_behavior_enriched_gongdan " +
                        "WHERE user_id IS NOT NULL " +
                        "GROUP BY user_id"
        );

        // 5. 注册为临时视图，再做 SELECT * 包一层变成 append-only
        tableEnv.createTemporaryView("funnel_result", funnelTable);
        Table appendOnlyTable = tableEnv.sqlQuery("SELECT * FROM funnel_result");

        // 6. 转为实体类
        SingleOutputStreamOperator<AdsUserFunnelConversion> resultStream =
                tableEnv.toDataStream(appendOnlyTable, Row.class)
                        .map(row -> {
                            AdsUserFunnelConversion funnel = new AdsUserFunnelConversion();
                            funnel.setUserId((String) row.getField(0));
                            funnel.setViewCnt(((Number) Objects.requireNonNull(row.getField(1))).longValue());
                            funnel.setCartCnt(((Number) Objects.requireNonNull(row.getField(2))).longValue());
                            funnel.setPayCnt(((Number) Objects.requireNonNull(row.getField(3))).longValue());
                            funnel.setViewToCartRate(((Number) Objects.requireNonNull(row.getField(4))).doubleValue());
                            funnel.setCartToPayRate(((Number) Objects.requireNonNull(row.getField(5))).doubleValue());
                            funnel.setViewToPayRate(((Number) Objects.requireNonNull(row.getField(6))).doubleValue());
                            return funnel;
                        }, Types.POJO(AdsUserFunnelConversion.class));

        // 7. 调试输出
        resultStream.print("funnel");

        // 8. 写入 ClickHouse
        resultStream.addSink(ClickHouseUtil.getSink(
                "INSERT INTO ads_user_funnel_conversion " +
                        "(user_id, view_cnt, cart_cnt, pay_cnt, view_to_cart_rate, cart_to_pay_rate, view_to_pay_rate) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)"
        ));

        // 9. 启动作业
        env.execute("Ads User Funnel Conversion Job");
    }
}
