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
import org.apache.flink.types.RowKind;

public class AdsUserPreferenceJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance().inStreamingMode().build()
        );

        // 2. 注册 Kafka 源表
        String groupId = "ads_user_preference_" + System.currentTimeMillis();
        tableEnv.executeSql(
                "CREATE TABLE dws_user_behavior_enriched_gongdan ( " +
                        "  user_id STRING, " +
                        "  behavior_type STRING, " +
                        "  sku_id_brand_id STRING, " +
                        "  category3_id STRING, " +
                        "  user_id_channel STRING, " +
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

        // 3. top_view_brand
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW top_view_brand AS " +
                        "SELECT user_id, sku_id_brand_id AS top_view_brand " +
                        "FROM ( " +
                        "  SELECT user_id, sku_id_brand_id, " +
                        "         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rk " +
                        "  FROM dws_user_behavior_enriched_gongdan " +
                        "  WHERE behavior_type = 'view' AND user_id IS NOT NULL AND sku_id_brand_id IS NOT NULL " +
                        "  GROUP BY user_id, sku_id_brand_id " +
                        ") t WHERE rk = 1"
        );

        // 4. top_cart_brand
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW top_cart_brand AS " +
                        "SELECT user_id, sku_id_brand_id AS top_cart_brand " +
                        "FROM ( " +
                        "  SELECT user_id, sku_id_brand_id, " +
                        "         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rk " +
                        "  FROM dws_user_behavior_enriched_gongdan " +
                        "  WHERE behavior_type = 'cart' AND user_id IS NOT NULL AND sku_id_brand_id IS NOT NULL " +
                        "  GROUP BY user_id, sku_id_brand_id " +
                        ") t WHERE rk = 1"
        );

        // 5. top_pay_brand
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW top_pay_brand AS " +
                        "SELECT user_id, sku_id_brand_id AS top_pay_brand " +
                        "FROM ( " +
                        "  SELECT user_id, sku_id_brand_id, " +
                        "         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rk " +
                        "  FROM dws_user_behavior_enriched_gongdan " +
                        "  WHERE behavior_type = 'pay' AND user_id IS NOT NULL AND sku_id_brand_id IS NOT NULL " +
                        "  GROUP BY user_id, sku_id_brand_id " +
                        ") t WHERE rk = 1"
        );

        // 6. top_category3_platform
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW top_category3_platform AS " +
                        "SELECT * FROM ( " +
                        "  SELECT user_id, category3_id AS prefer_category3, user_id_channel AS prefer_platform, " +
                        "         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) AS rk " +
                        "  FROM dws_user_behavior_enriched_gongdan " +
                        "  WHERE user_id IS NOT NULL AND category3_id IS NOT NULL AND user_id_channel IS NOT NULL " +
                        "  GROUP BY user_id, category3_id, user_id_channel " +
                        ") t WHERE rk = 1"
        );

        // 7. join 所有视图
        Table resultTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  DISTINCT(v.user_id), " +
                        "  v.top_view_brand, " +
                        "  c.top_cart_brand, " +
                        "  p.top_pay_brand, " +
                        "  cp.prefer_category3, " +
                        "  cp.prefer_platform " +
                        "FROM top_view_brand v " +
                        "LEFT JOIN top_cart_brand c ON v.user_id = c.user_id " +
                        "LEFT JOIN top_pay_brand p ON v.user_id = p.user_id " +
                        "LEFT JOIN top_category3_platform cp ON v.user_id = cp.user_id " +
                        "LIMIT 5000"
        );

        // 8. 转换为 DataStream<POJO>
        SingleOutputStreamOperator<AdsUserPreference> stream = tableEnv.toChangelogStream(resultTable)
                .filter(row -> row.getKind() == RowKind.INSERT)
                .map(row -> {
                    AdsUserPreference bean = new AdsUserPreference();
                    bean.setUserId((String) row.getField(0));
                    bean.setTopViewBrand((String) row.getField(1));
                    bean.setTopCartBrand((String) row.getField(2));
                    bean.setTopPayBrand((String) row.getField(3));
                    bean.setPreferCategory3((String) row.getField(4));
                    bean.setPreferPlatform((String) row.getField(5));
                    return bean;
                }, Types.POJO(AdsUserPreference.class))
                .filter(bean ->
                        bean.getUserId() != null &&
                                bean.getTopViewBrand() != null &&
                                bean.getTopCartBrand() != null &&
                                bean.getTopPayBrand() != null &&
                                bean.getPreferCategory3() != null &&
                                bean.getPreferPlatform() != null
                );
// 添加全局限制：只保留前 5000 条记录
        SingleOutputStreamOperator<AdsUserPreference> limitedStream = stream
                .keyBy(bean -> true) // 所有数据都进同一个 key
                .process(new LimitProcessFunction<AdsUserPreference>(5000))
                .returns(Types.POJO(AdsUserPreference.class)); //  加这行解决类型推断失败


        limitedStream.print("preference");

        // 9. 写入 ClickHouse
        limitedStream.addSink(ClickHouseUtil.getSink(
                "INSERT INTO ads_user_preference (user_id, top_view_brand, top_cart_brand, top_pay_brand, prefer_category3, prefer_platform) VALUES (?, ?, ?, ?, ?, ?)",
                100, 2000
        ));


        // 10. 启动执行
        env.execute("AdsUserPreferenceJob");
    }
}
