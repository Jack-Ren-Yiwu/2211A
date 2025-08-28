package com.app.ads;

import com.app.bean.AdsUserPreference;
import com.stream.common.utils.ClickHouseUtil;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class AdsUserPreferenceJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // 2. 注册 Kafka 表
        String groupId = "ads_user_preference_" + System.currentTimeMillis();
        tableEnv.executeSql(
                "CREATE TABLE dws_user_behavior_enriched_gongdan ( " +
                        "  user_id STRING, " +
                        "  behavior_type STRING, " +
                        "  brand_id STRING, " +
                        "  category3_id STRING, " +
                        "  platform STRING, " +
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

        // 3. 计算每种行为 top1 品牌
        tableEnv.executeSql(
                "CREATE TEMPORARY  VIEW top_brands AS " +
                        "SELECT * FROM ( " +
                        "  SELECT " +
                        "    user_id, behavior_type, brand_id, " +
                        "    ROW_NUMBER() OVER (PARTITION BY user_id, behavior_type ORDER BY COUNT(*) DESC) AS rk " +
                        "  FROM dws_user_behavior_enriched_gongdan " +
                        "  WHERE user_id IS NOT NULL AND brand_id IS NOT NULL " +
                        "  GROUP BY user_id, behavior_type, brand_id " +
                        ") tmp WHERE rk = 1"
        );

        // 4. 分别提取 view/cart/pay 的 top 品牌
        tableEnv.executeSql(
                "CREATE TEMPORARY  VIEW view_top AS " +
                        "SELECT user_id, brand_id AS top_view_brand FROM top_brands WHERE behavior_type = 'view'"
        );

        tableEnv.executeSql(
                "CREATE TEMPORARY  VIEW cart_top AS " +
                        "SELECT user_id, brand_id AS top_cart_brand FROM top_brands WHERE behavior_type = 'cart'"
        );

        tableEnv.executeSql(
                "CREATE TEMPORARY  VIEW pay_top AS " +
                        "SELECT user_id, brand_id AS top_pay_brand FROM top_brands WHERE behavior_type = 'pay'"
        );

        // 5. 偏好类目、平台：直接最多次数统计
        tableEnv.executeSql(
                "CREATE TEMPORARY  VIEW top_category_platform AS " +
                        "SELECT * FROM ( " +
                        "  SELECT user_id, category3_id, platform, " +
                        "         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rk " +
                        "  FROM dws_user_behavior_enriched_gongdan " +
                        "  WHERE category3_id IS NOT NULL AND platform IS NOT NULL " +
                        "  GROUP BY user_id, category3_id, platform " +
                        ") tmp WHERE rk = 1"
        );

        // 6. 最终表拼接
        Table result = tableEnv.sqlQuery(
                "SELECT " +
                        "COALESCE(v.user_id, c.user_id, p.user_id, cp.user_id) AS user_id, " +
                        "v.top_view_brand, c.top_cart_brand, p.top_pay_brand, " +
                        "cp.category3_id AS prefer_category3, cp.platform AS prefer_platform " +
                        "FROM view_top v " +
                        "FULL OUTER JOIN cart_top c ON v.user_id = c.user_id " +
                        "FULL OUTER JOIN pay_top p ON COALESCE(v.user_id, c.user_id) = p.user_id " +
                        "FULL OUTER JOIN top_category_platform cp ON COALESCE(v.user_id, c.user_id, p.user_id) = cp.user_id"
        );

        // 7. 转换为 DataStream<POJO>
        SingleOutputStreamOperator<AdsUserPreference> stream = tableEnv.toDataStream(result, Row.class)
                .map(row -> {
                    AdsUserPreference bean = new AdsUserPreference();
                    bean.setUserId((String) row.getField(0));
                    bean.setTopViewBrand((String) row.getField(1));
                    bean.setTopCartBrand((String) row.getField(2));
                    bean.setTopPayBrand((String) row.getField(3));
                    bean.setPreferCategory3((String) row.getField(4));
                    bean.setPreferPlatform((String) row.getField(5));
                    return bean;
                }, Types.POJO(AdsUserPreference.class));

        // 8. 写入 ClickHouse
        stream.addSink(ClickHouseUtil.getSink(
                "INSERT INTO ads_user_preference " +
                        "(user_id, top_view_brand, top_cart_brand, top_pay_brand, prefer_category3, prefer_platform) " +
                        "VALUES (?, ?, ?, ?, ?, ?)"
        ));

        // 9. 启动作业
        env.execute("Ads User Preference Job");
    }
}
