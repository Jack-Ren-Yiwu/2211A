package com.app.tag;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class UserTagStreamJob {

    private static final Logger log = LoggerFactory.getLogger(UserTagStreamJob.class);
   // private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStream<String> behaviorStream = env.fromSource(
                KafkaUtils.getKafkaSource("dws_user_behavior_enriched_gongdan", "user_tag_group", "cdh01:9092"),
                WatermarkStrategy.noWatermarks(),
                "behavior-source"
        );

        DataStream<String> orderStream = env.fromSource(
                KafkaUtils.getKafkaSource("dws_order_enriched_gongdan", "user_tag_group", "cdh01:9092"),
                WatermarkStrategy.noWatermarks(),
                "order-source"
        );

        DataStream<JSONObject> merged = behaviorStream.union(orderStream)
                .map(JSON::parseObject)
                .filter(json -> json.containsKey("user_id") && json.getString("user_id") != null);

        merged.keyBy(json -> json.getString("user_id"))
                .process(new UserTagProcessFunction());

        env.execute("UserTagStreamJob");
    }

    public static class UserTagProcessFunction extends KeyedProcessFunction<String, JSONObject, Void> {

        private transient Connection conn;
        private transient MapState<String, JSONObject> behaviorState;
        private transient MapState<String, JSONObject> orderState;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration hConf = HBaseConfiguration.create();
            hConf.set("hbase.zookeeper.quorum", "cdh01:2181");
            conn = ConnectionFactory.createConnection(hConf);

            behaviorState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("behavior", Types.STRING, Types.GENERIC(JSONObject.class)));
            orderState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("order", Types.STRING, Types.GENERIC(JSONObject.class)));
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<Void> out) throws Exception {
            String userId = value.getString("user_id");
            if (userId == null) {
                log.warn("缺失 user_id，跳过该条记录: {}", value);
                return;
            }

            if (value.containsKey("behavior_type")) {
                behaviorState.put("latest", value);
            } else if (value.containsKey("order_id")) {
                orderState.put("latest", value);
            }

            JSONObject behavior = behaviorState.get("latest");
            //JSONObject order = orderState.get("latest");

            JSONObject tag = new JSONObject();
            tag.put("user_id", userId);
            tag.put("gender", getOrDefault(behavior, "user_id_gender", "未知"));
            tag.put("age", getOrDefault(behavior, "user_id_age", "未知"));
            tag.put("level", getOrDefault(behavior, "user_id_level", "未知"));
            tag.put("province", getOrDefault(behavior, "user_id_province", "未知"));
            tag.put("channel", getOrDefault(behavior, "user_id_channel", "未知"));
            tag.put("is_blacklist", getOrDefault(behavior, "user_id_is_blacklist", "0"));
            tag.put("top_click_category3", getOrDefault(behavior, "category3_id_category3_id", "无"));
            tag.put("top_click_category2", getOrDefault(behavior, "category3_id_category2_id", "无"));
            tag.put("top_click_category1", getOrDefault(behavior, "category3_id_category1_id", "无"));
            tag.put("top_brand", getOrDefault(behavior, "sku_id_brand_id", "无"));



            try {
                writeToHBase(userId, tag);
            } catch (Exception e) {
                log.error("HBase 写入失败，user_id={}，tag={}", userId, tag.toJSONString(), e);
            }
        }

        private String getOrDefault(JSONObject obj, String key, String defaultVal) {
            return (obj != null && obj.containsKey(key) && obj.getString(key) != null)
                    ? obj.getString(key)
                    : defaultVal;
        }

        private void writeToHBase(String rowKey, JSONObject tag) throws Exception {
            Table table = conn.getTable(TableName.valueOf("default:dim_user_tag"));
            Put put = new Put(Bytes.toBytes(rowKey));
            for (String key : tag.keySet()) {
                if (!"user_id".equals(key)) {
                    String val = tag.getString(key);
                    if (val != null) {
                        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(key), Bytes.toBytes(val));
                    }
                }
            }
            table.put(put);
            table.close();
            log.info("✅ 用户标签写入完成：user_id={}, 标签字段={}", rowKey, tag.toJSONString());
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
        }
    }
}
