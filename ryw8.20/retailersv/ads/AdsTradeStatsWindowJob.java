package com.retailersv.ads;

import com.retailersv.bean.EnrichedStats;
import com.retailersv.bean.OrderStats;
import com.retailersv.bean.PaymentStats;
import com.retailersv.bean.RefundStats;
import com.stream.common.utils.ClickHouseUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.RowKind;

public class AdsTradeStatsWindowJob {
    public static void main(String[] args) throws Exception {

        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setParallelism(2);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-ads");

        // 2. 创建 Kafka 源表
        tableEnv.executeSql(
                "CREATE TABLE dwd_order_detail_enriched (\n" +
                        "  sku_id STRING,\n" +
                        "  category3_id STRING,\n" +
                        "  tm_id STRING,\n" +
                        "  user_id STRING,\n" +
                        "  order_id STRING,\n" +
                        "  order_detail_id STRING,\n" +
                        "  order_amount DOUBLE,\n" +
                        "  payment_amount DOUBLE,\n" +
                        "  refund_amount DOUBLE,\n" +
                        "  event_type STRING,\n" +
                        "  proc_time AS PROCTIME()\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'dwd_order_detail_enriched',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'properties.group.id' = 'ads_trade_stats',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );
//                Table origin = tableEnv.sqlQuery("SELECT * FROM dwd_order_detail_enriched");
//        tableEnv.toDataStream(origin).print("原始数据");

//        tableEnv.executeSql("SELECT event_type, COUNT(*) \n" +
//                "FROM dwd_order_detail_enriched \n" +
//                "GROUP BY event_type").print();
        // 3. 下单情况统计
        Table orderStatsTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        "  sku_id,\n" +
                        "  COUNT(*) AS order_ct,\n" +
                        "  COUNT(DISTINCT user_id) AS order_user_ct,\n" +
                        "  SUM(CAST(1 AS BIGINT)) AS sku_num,\n" +
                        "  SUM(order_amount) AS order_amount\n" +
                        "FROM TABLE(\n" +
                        "  TUMBLE(TABLE dwd_order_detail_enriched, DESCRIPTOR(proc_time), INTERVAL '10' SECOND))\n" +
                        "WHERE event_type = 'order'\n" +
                        "GROUP BY window_start, window_end, sku_id, category3_id, tm_id"
        );
       // tableEnv.toDataStream(orderStatsTable).print("order_stats");

        // 4. 支付情况统计
        Table paymentStatsTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        "  sku_id,\n" +
                        "  COUNT(*) AS payment_ct,\n" +
                        "  COUNT(DISTINCT user_id) AS payment_user_ct,\n" +
                        "  ROUND(SUM(payment_amount),2) AS payment_amount\n" +
                        "FROM TABLE(\n" +
                        "  TUMBLE(TABLE dwd_order_detail_enriched, DESCRIPTOR(proc_time), INTERVAL '10' SECOND))\n" +
                        "WHERE event_type = 'payment'\n" +
                        "GROUP BY window_start, window_end, sku_id, category3_id, tm_id"
        );
        //tableEnv.toDataStream(paymentStatsTable).print("payment_stats");

        // 5. 退款情况统计
        Table refundStatsTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        "  sku_id,\n" +
                        "  COUNT(*) AS refund_ct,\n" +
                        "  COUNT(DISTINCT user_id) AS refund_user_ct,\n" +
                        "  SUM(refund_amount) AS refund_amount\n" +
                        "FROM TABLE(\n" +
                        "  TUMBLE(TABLE dwd_order_detail_enriched, DESCRIPTOR(proc_time), INTERVAL '10' SECOND))\n" +
                        "WHERE event_type = 'refund'\n" +
                        "GROUP BY window_start, window_end, sku_id, category3_id, tm_id"
        );
      //tableEnv.toDataStream(refundStatsTable).print("refund_stats");


        // 6. 注册临时视图
        tableEnv.createTemporaryView("order_stats", orderStatsTable);
        tableEnv.createTemporaryView("payment_stats", paymentStatsTable);
        tableEnv.createTemporaryView("refund_stats", refundStatsTable);

        // 7. 汇总指标表
        Table resultStatsTable = tableEnv.sqlQuery(
                "SELECT \n" +
                        "  o.stt,\n" +
                        "  o.edt,\n" +
                        "  o.sku_id,\n" +
                        "  o.order_ct,\n" +
                        "  o.order_user_ct,\n" +
                        "  o.sku_num,\n" +
                        "  ROUND(o.order_amount, 2) AS order_amount,\n" +
                        "  r.refund_ct,\n" +
                        "  r.refund_user_ct,\n" +
                        "  ROUND(r.refund_amount, 2) AS refund_amount\n" +
                        "FROM order_stats o\n" +
                        "LEFT JOIN payment_stats p ON o.stt = p.stt AND o.edt = p.edt AND o.sku_id = p.sku_id\n" +
                        "LEFT JOIN refund_stats r ON o.stt = r.stt AND o.edt = r.edt AND o.sku_id = r.sku_id"
        );

        // 8. 输出 enriched 汇总表
       // tableEnv.toChangelogStream(resultStatsTable).print("enriched_trade_stats");

// 1. 下单 sink
        tableEnv.toDataStream(orderStatsTable, OrderStats.class)
                .addSink(ClickHouseUtil.getSink(
                        "INSERT INTO order_stats VALUES (?, ?, ?, ?, ?, ?, ?)"
                ));

// 2. 支付 sink
        tableEnv.toDataStream(paymentStatsTable, PaymentStats.class)
                .addSink(ClickHouseUtil.getSink(
                        "INSERT INTO payment_stats VALUES (?, ?, ?, ?, ?, ?)"
                ));

// 3. 退款 sink
        tableEnv.toDataStream(refundStatsTable, RefundStats.class)
                .addSink(ClickHouseUtil.getSink(
                        "INSERT INTO refund_stats VALUES (?, ?, ?, ?, ?, ?)"
                ));

// 4. 汇总 sink
        tableEnv.toChangelogStream(resultStatsTable)
                .filter(row -> row.getKind() == RowKind.INSERT || row.getKind() == RowKind.UPDATE_AFTER)
                .map(row -> EnrichedStats.builder()
                        .stt(row.getFieldAs("stt"))
                        .edt(row.getFieldAs("edt"))
                        .sku_id(row.getFieldAs("sku_id"))
                        .order_ct(row.getFieldAs("order_ct"))
                        .order_user_ct(row.getFieldAs("order_user_ct"))
                        .sku_num(row.getFieldAs("sku_num"))
                        .order_amount(row.getFieldAs("order_amount"))
                        .refund_ct(row.getFieldAs("refund_ct"))
                        .refund_user_ct(row.getFieldAs("refund_user_ct"))
                        .refund_amount(row.getFieldAs("refund_amount"))
                        .build()
                )
                .addSink(ClickHouseUtil.getSink(
                        "INSERT INTO enriched_trade_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                ));


        // 6. 启动作业
        env.execute("AdsTradeStatsWindowJob");
    }
}
