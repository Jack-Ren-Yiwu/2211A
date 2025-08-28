package com.app.ads;

import com.app.bean.AdsUserOrderStats;
import com.stream.common.utils.ClickHouseUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Objects;

public class AdsUserOrderStatsJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 动态 groupId
        String groupId = "ads_user_order_stats_debug_" + System.currentTimeMillis();

        // 3. 注册 Kafka Source 表
        tableEnv.executeSql(
                "CREATE TABLE dws_order_enriched_gongdan ( " +
                        "  user_id STRING, " +
                        "  order_id STRING, " +
                        "  order_amount DOUBLE, " +
                        "  refund_amount DOUBLE, " +
                        "  coupon_amount DOUBLE, " +
                        "  payment_channel STRING, " +
                        "  user_id_register_time BIGINT, " +
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

        // 4. 聚合窗口，得到统计结果表（SQL 查询）
        Table resultTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  user_id, " +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start, " +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end, " +
                        "  COUNT(DISTINCT order_id) AS order_ct, " +
                        "  SUM(CASE WHEN refund_amount > 0 THEN 1 ELSE 0 END) AS refund_ct, " +
                        "  SUM(order_amount) AS order_amount_sum, " +
                        "  SUM(refund_amount) AS refund_amount_sum, " +
                        "  SUM(coupon_amount) AS coupon_amount_sum " +
                        "FROM TABLE( " +
                        "  TUMBLE(TABLE dws_order_enriched_gongdan, DESCRIPTOR(proctime), INTERVAL '1' MINUTE) " +
                        ") " +
                        "WHERE user_id IS NOT NULL " +
                        "GROUP BY window_start, window_end, user_id"
        );

        // 5. 转为实体类 DataStream<AdsUserOrderStats>
        SingleOutputStreamOperator<AdsUserOrderStats> resultStream =
                tableEnv.toDataStream(resultTable, Row.class)
                        .map(row -> {
                            AdsUserOrderStats stats = new AdsUserOrderStats();
                            stats.setUserId((String) row.getField(0));
                            stats.setWindowStart((String) row.getField(1));
                            stats.setWindowEnd((String) row.getField(2));
                            stats.setOrderCt(((Number) Objects.requireNonNull(row.getField(3))).longValue());
                            stats.setRefundCt(((Number) Objects.requireNonNull(row.getField(4))).longValue());
                            stats.setOrderAmountSum((Double) row.getField(5));
                            stats.setRefundAmountSum((Double) row.getField(6));
                            stats.setCouponAmountSum((Double) row.getField(7));
                            return stats;
                        }, Types.POJO(AdsUserOrderStats.class));


        // 6. 调试输出
        resultStream.print("window_agg");

        // 7. 写入 ClickHouse
        resultStream.addSink(ClickHouseUtil.getSink(
                "INSERT INTO ads_user_order_stats " +
                        "(user_id, window_start, window_end, order_ct, refund_ct, order_amount_sum, refund_amount_sum, coupon_amount_sum) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        ));

        // 启动任务
        env.execute();
    }
}
