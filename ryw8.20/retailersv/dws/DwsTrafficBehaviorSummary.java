package com.retailersv.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;


public class DwsTrafficBehaviorSummary {

    public static void main(String[] args) throws Exception {
        // 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-dws");

        // 2. 从 Kafka 读取各类 DWD 流（假设均为 JSON 字符串）
        // 替换为你实际的 Kafka source 工具类
        String kafkaServer = "cdh01:9092";
        // 从 Kafka 构建各个 DWD 层的流
        DataStream<String> startStream = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_start_log", "dwd_start_log", kafkaServer),
                WatermarkStrategy.noWatermarks(),
                "dwd_start_log");

        DataStream<String> pageStream = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_traffic_page_log", "dwd_traffic_page_log", kafkaServer),
                WatermarkStrategy.noWatermarks(),
                "dwd_traffic_page_log");

        DataStream<String> displayStream = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_traffic_display_log", "dwd_traffic_display_log", kafkaServer),
                WatermarkStrategy.noWatermarks(),
                "dwd_traffic_display_log");

        DataStream<String> actionStream = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_action_log", "dwd_action_log", kafkaServer),
                WatermarkStrategy.noWatermarks(),
                "dwd_action_log");


        // 4. 按设备 ID 分组
        DataStream<JSONObject> unifiedStream = startStream
                .map(JSON::parseObject)
                .map(json -> {
                    json.put("event_type", "start");
                    json.put("mid", json.getJSONObject("common").getString("mid"));
                    json.put("event_ts", json.getLong("ts"));
                    return json;
                })
                .union(
                        pageStream.map(JSON::parseObject).map(json -> {
                            json.put("event_type", "page");
                            json.put("mid", json.getJSONObject("common").getString("mid"));
                            json.put("event_ts", json.getLong("ts"));
                            return json;
                        }),
                        displayStream.map(JSON::parseObject).map(json -> {
                            json.put("event_type", "display");
                            json.put("mid", json.getJSONObject("common").getString("mid"));
                            json.put("event_ts", json.getLong("ts"));
                            return json;
                        }),
                        actionStream.map(JSON::parseObject).map(json -> {
                            json.put("event_type", "action");
                            // ts 要从 action 字段里取
                            JSONObject common = json.getJSONObject("common");
                            JSONObject action = json.getJSONObject("action");
                            json.put("mid", common.getString("mid"));
                            json.put("event_ts", action.getLongValue("ts"));
                            return json;
                        })
                );
        unifiedStream.print();

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dws_user_action_detail")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setKafkaProducerConfig(new Properties() {{
                    put(ProducerConfig.ACKS_CONFIG, "all");
                }})
                .build();

        unifiedStream.map(JSON::toJSONString).sinkTo(kafkaSink);
        env.execute("DWS Traffic Behavior Summary");

    }

}
