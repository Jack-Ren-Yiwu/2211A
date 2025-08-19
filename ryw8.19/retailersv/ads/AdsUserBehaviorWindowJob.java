package com.retailersv.ads;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AdsUserBehaviorWindowJob {
    public static void main(String[] args) throws Exception {

        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-ads");
        env.setParallelism(2);
        tableEnv.getConfig().getConfiguration().setString("table.exec.window-agg.buffer-size.limit", "2000000");
        tableEnv.getConfig().getConfiguration().setString("table.exec.resource.default-parallelism", "2");

        // 2. 创建 Kafka 源表
        tableEnv.executeSql(
                "CREATE TABLE dws_user_action_enriched (\n" +
                        "  event_type STRING,\n" +
                        "  common ROW<ar STRING, uid STRING, ch STRING, is_new STRING, vc STRING, ba STRING, os STRING, md STRING, mid STRING, sid STRING>,\n" +
                        "  page ROW<page_id STRING, last_page_id STRING, during_time BIGINT>,\n" +
                        "  display ROW<item STRING, item_type STRING, pos_id INT>,\n" +
                        "  ts BIGINT,\n" +
                        "  proctime AS PROCTIME()\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'dws_user_action_enriched',\n" +
                        "  'properties.bootstrap.servers' = 'cdh01:9092',\n" +
                        "  'properties.group.id' = 'ads_user_behavior_window',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json',\n" +
                        "  'json.ignore-parse-errors' = 'true'\n" +
                        ")"
        );

        // 打印原始数据
//        Table origin = tableEnv.sqlQuery("SELECT * FROM dws_user_action_enriched");
//        tableEnv.toDataStream(origin).print("原始数据");

        // 3. PV/UV 查询
        Table pvUvTable = tableEnv.sqlQuery(
                "SELECT\n" +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        "  common.vc, common.ch, common.ar, common.is_new,\n" +
                        "  COUNT(*) AS pv_ct,\n" +
                        "  COUNT(DISTINCT common.uid) AS uv_ct\n" +
                        "FROM TABLE(\n" +
                        "  TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)\n" +
                        ")\n" +
                        "GROUP BY window_start, window_end, common.vc, common.ch, common.ar, common.is_new"
        );
        tableEnv.toDataStream(pvUvTable).print("pv_uv");

        // 4. 会话指标（启动事件）
        Table sessionTable = tableEnv.sqlQuery(
                "SELECT\n" +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        "  common.vc, common.ch, common.ar, common.is_new,\n" +
                        "  COUNT(*) AS sv_ct,\n" +
                        "  SUM(CASE WHEN page.last_page_id IS NULL THEN 1 ELSE 0 END) AS uj_ct,\n" +
                        "  SUM(page.during_time) AS dur_sum\n" +
                        "FROM TABLE(\n" +
                        "  TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)\n" +
                        ")\n" +
                        "WHERE event_type = 'start'\n" +
                        "GROUP BY window_start, window_end, common.vc, common.ch, common.ar, common.is_new"
        );
        tableEnv.toDataStream(sessionTable).print("session");

        // 5. 曝光/点击指标
        Table displayClickTable = tableEnv.sqlQuery(
                "SELECT\n" +
                        "  DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS stt,\n" +
                        "  DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS edt,\n" +
                        "  display.item AS sku_id,\n" +
                        "  page.page_id,\n" +
                        "  common.ch,\n" +
                        "  COUNT(CASE WHEN event_type = 'display' THEN 1 END) AS disp_ct,\n" +
                        "  COUNT(DISTINCT CASE WHEN event_type = 'display' THEN display.item END) AS disp_sku_num,\n" +
                        "  COUNT(CASE WHEN event_type = 'click' THEN 1 END) AS click_ct,\n" +
                        "  COUNT(DISTINCT CASE WHEN event_type = 'click' THEN display.item END) AS click_sku_num\n" +
                        "FROM TABLE(\n" +
                        "  TUMBLE(TABLE dws_user_action_enriched, DESCRIPTOR(proctime), INTERVAL '10' SECOND)\n" +
                        ")\n" +
                        "GROUP BY window_start, window_end, display.item, page.page_id, common.ch"
        );
        tableEnv.toDataStream(displayClickTable).print("disp_click");

        // 6. 启动作业
        env.execute("AdsUserBehaviorWindowJob");
    }
}
