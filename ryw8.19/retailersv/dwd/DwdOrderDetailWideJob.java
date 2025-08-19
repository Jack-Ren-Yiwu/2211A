package com.retailersv.dwd;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdOrderDetailWideJob {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat("taskmanager.memory.network.fraction", 0.3f);
        conf.setString("taskmanager.memory.network.min", "256mb");
        conf.setString("taskmanager.memory.network.max", "256mb");

        // 1. 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 2. 注册 Kafka 源表（Debezium 格式）
        tableEnv.executeSql(
                "CREATE TABLE ods_order (\n" +
                        "  `op` STRING,\n" +
                        "  `after` ROW<\n" +
                        "    id STRING,\n" +
                        "    order_id STRING,\n" +
                        "    order_detail_id STRING,\n" +
                "    sku_id STRING,\n" +
                        "    sku_name STRING,\n" +
                        "    order_price STRING,\n" +
                        "    sku_num STRING,\n" +
                        "    create_time STRING,\n" +
                        "    user_id STRING,\n" +
                        "    province_id STRING,\n" +
                        "    activity_id STRING,\n" +
                        "    activity_rule_id STRING,\n" +
                "    coupon_id STRING,\n" +
                        "    coupon_use_id STRING,\n" +
                "    payment_type STRING,\n" +
                        "    callback_time STRING,\n" +
                        "    total_amount STRING,\n" +
                        "    refund_amount STRING,\n" +
                        "    refund_reason_type STRING,\n" +
                        "    refund_reason_txt STRING,\n" +
                "    refund_create_time STRING\n" +
                        "  >,\n" +
                        "  `source` MAP<STRING, STRING>,\n" +
                        "  `ts_ms` BIGINT,\n" +
                        "  `proc_time` AS PROCTIME()\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'ods_ecommerce_order',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'properties.group.id' = 'flink_order_consumer',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );


        // 3. 拆分多张临时视图（通过 source.table 区分）
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_detail AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_detail'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_info AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_info'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_detail_coupon AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_detail_coupon'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_detail_activity AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_detail_activity'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW payment_info AS SELECT * FROM ods_order WHERE `source`['table'] = 'payment_info'");
        tableEnv.executeSql("CREATE TEMPORARY VIEW order_refund_info AS SELECT * FROM ods_order WHERE `source`['table'] = 'order_refund_info'");

        // 4. 拉宽为 dwd_order_detail_wide
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW dwd_order_detail_wide AS\n" +
                        "SELECT\n" +
                        "  od.after.id                       AS order_detail_id,\n" +
                        "  od.after.order_id                AS order_id,\n" +
                        "  od.after.sku_id                  AS sku_id,\n" +
                        "  od.after.sku_name                AS sku_name,\n" +
                        "  od.after.order_price             AS order_price,\n" +
                        "  od.after.sku_num                 AS sku_num,\n" +
                        "  od.after.create_time             AS order_detail_create_time,\n" +
                        "  oi.after.user_id                 AS user_id,\n" +
                        "  oi.after.province_id             AS province_id,\n" +
                        "  oi.after.create_time             AS order_create_time,\n" +
                        "  act.after.activity_id            AS activity_id,\n" +
                        "  cou.after.coupon_id              AS coupon_id,\n" +
                        "  pay.after.payment_type           AS payment_type,\n" +
                        "  pay.after.callback_time          AS payment_time,\n" +
                        "  pay.after.total_amount           AS pay_amount,\n" +
                        "  refund.after.refund_amount       AS refund_amount,\n" +
                        "  refund.after.refund_reason_type  AS refund_reason_type,\n" +
                        "  refund.after.refund_create_time  AS refund_create_time,\n" +
                        "  od.ts_ms                         AS ts,\n" +
                        "  od.proc_time                     AS proc_time\n" +
                        "FROM order_detail od\n" +
                        "LEFT JOIN order_info oi ON od.after.order_id = oi.after.id\n" +
                        "LEFT JOIN order_detail_activity act ON od.after.id = act.after.order_detail_id\n" +
                        "LEFT JOIN order_detail_coupon cou ON od.after.id = cou.after.order_detail_id\n" +
                        "LEFT JOIN payment_info pay ON od.after.order_id = pay.after.order_id\n" +
                        "LEFT JOIN order_refund_info refund ON od.after.order_id = refund.after.order_id"
        );


        // 5. 输出验证
     //  tableEnv.executeSql("SELECT count(*) FROM dwd_order_detail_wide WHERE user_id IS NOT NULL AND pay_amount IS NOT NULL").print();

        // 6. 建立 Kafka Sink 表
        tableEnv.executeSql(
                "CREATE TABLE dwd_order_detail_wide_kafka (\n" +
                        "  order_detail_id STRING,\n" +
                        "  order_id STRING,\n" +
                        "  sku_id STRING,\n" +
                        "  sku_name STRING,\n" +
                        "  order_price STRING,\n" +
                        "  sku_num STRING,\n" +
                        "  order_detail_create_time STRING,\n" +
                        "  user_id STRING,\n" +
                        "  province_id STRING,\n" +
                        "  order_create_time STRING,\n" +
                        "  activity_id STRING,\n" +
                        "  coupon_id STRING,\n" +
                        "  payment_type STRING,\n" +
                        "  payment_time STRING,\n" +
                        "  pay_amount STRING,\n" +
                        "  refund_amount STRING,\n" +
                        "  refund_reason_type STRING,\n" +
                        "  refund_create_time STRING,\n" +
                        "  ts BIGINT,\n" +
                        "  proc_time TIMESTAMP(3),\n" +
                        "  PRIMARY KEY (order_detail_id) NOT ENFORCED\n" +  // 必须添加主键
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'dwd_order_detail_wide',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")"
        );


        // 7. 写入 Kafka sink 表
        tableEnv.executeSql("INSERT INTO dwd_order_detail_wide_kafka\n" +
                "SELECT *\n" +
                "FROM dwd_order_detail_wide\n" +
                "WHERE user_id IS NOT NULL AND pay_amount IS NOT NULL");



    }
}
