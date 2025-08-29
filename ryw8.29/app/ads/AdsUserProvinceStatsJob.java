package com.app.ads;

import com.app.bean.AdsUserProvinceStats;
import com.stream.common.utils.ClickHouseUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.RowKind;

public class AdsUserProvinceStatsJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // 2. 注册 Kafka 表（用户订单宽表，包含订单、支付、退款信息）
        String groupId = "ads_user_province_stats_" + System.currentTimeMillis();
        tableEnv.executeSql(
                "CREATE TABLE dws_order_enriched_gongdan ( " +
                        "  user_id STRING, " +
                        "  user_id_province STRING, " +
                        "  order_id STRING, " +
                        "  order_amount DOUBLE, " +
                        "  refund_type STRING, " +
                        "  refund_amount DOUBLE, " +
                        "  pay_time AS PROCTIME() " +
                        ") WITH ( " +
                        "  'connector' = 'kafka', " +
                        "  'topic' = 'dws_order_enriched_gongdan', " +
                        "  'properties.bootstrap.servers' = 'cdh01:9092', " +
                        "  'properties.group.id' = '" + groupId + "', " +
                        "  'scan.startup.mode' = 'earliest-offset', " +
                        "  'format' = 'json' " +
                        ")"
        );

        // 3. 聚合计算省份用户统计指标
        Table resultTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  user_id_province, " +
                        "  COUNT(DISTINCT user_id) AS user_cnt, " +
                        "  SUM(order_amount) AS order_amount, " +
                        "  COUNT(DISTINCT CASE WHEN pay_time IS NOT NULL THEN user_id ELSE NULL END) AS pay_user_cnt, " +
                        "  SUM(refund_amount) AS refund_amount " +
                        "FROM dws_order_enriched_gongdan " +
                        "WHERE user_id_province IS NOT NULL " +
                        "GROUP BY user_id_province"
        );

        // 4. 转换为 DataStream
        SingleOutputStreamOperator<AdsUserProvinceStats> stream = tableEnv.toChangelogStream(resultTable)
                .filter(row -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER)
                .map(row -> {
                    AdsUserProvinceStats bean = new AdsUserProvinceStats();
                    bean.setProvince((String) row.getField(0));
                    bean.setUserCnt((Long) row.getField(1));
                    bean.setOrderAmount((Double) row.getField(2));
                    bean.setPayUserCnt((Long) row.getField(3));
                    bean.setRefundAmount((Double) row.getField(4));
                    return bean;
                }, Types.POJO(AdsUserProvinceStats.class))
                .filter(bean ->
                        bean.getProvince() != null &&
                                bean.getUserCnt() != null &&
                                bean.getOrderAmount() != null &&
                                bean.getPayUserCnt() != null &&
                                bean.getRefundAmount() != null
                );

        stream.print("province_stats");

        // 5. 写入 ClickHouse
        stream.addSink(ClickHouseUtil.getSink(
                "INSERT INTO ads_user_province_stats " +
                        "(province, user_cnt, order_amount, pay_user_cnt, refund_amount) " +
                        "VALUES (?, ?, ?, ?, ?)",
                100,
                2000
        ));

        // 6. 启动作业
        env.execute("AdsUserProvinceStatsJob");
    }
}
