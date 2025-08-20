package com.retailersv.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.Map;

public class DwdTopicPreviewApp {
    private static final String KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-DWS");

        String[] topics = {
                "dwd_trade_order_detail",
                "dwd_trade_pay_detail",
                "dwd_trade_order_refund",
                "dwd_user_register",
                "dwd_interaction_favor_add",
                "dwd_tool_coupon_get",
                "dwd_dynamic_fallback"
        };

        for (String topic : topics) {
            previewOneMessage(env, topic);
        }

        env.execute("DwdTopicPreviewApp");
    }

    private static void previewOneMessage(StreamExecutionEnvironment env, String topic) {
        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_SERVER, topic, "preview_group_" + topic, OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                topic + "_source"
        );

        source.flatMap(new OneMessagePrinter(topic));
    }

    static class OneMessagePrinter extends RichFlatMapFunction<String, String> {
        private static final Map<String, Boolean> printed = new HashMap<>();
        private final String topic;

        public OneMessagePrinter(String topic) {
            this.topic = topic;
        }

        @Override
        public void open(Configuration parameters) {
            printed.putIfAbsent(topic, false);
        }

        @Override
        public void flatMap(String value, Collector<String> out) {
            if (!printed.get(topic)) {
                out.collect(topic + " -> " + value);
                printed.put(topic, true);
            }
        }
    }
}
