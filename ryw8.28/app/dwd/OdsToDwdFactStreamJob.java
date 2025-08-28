package com.app.dwd;

import com.alibaba.fastjson.JSON;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OdsToDwdFactStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        String kafkaServers = "cdh01:9092";  // 改成你实际的 Kafka 集群地址
        String odsTopic = "ods_gongdan07_order";
        String groupId = "ods_to_dwd_group";

        DataStreamSource<String> sourceStream = env.fromSource(
                KafkaUtils.getKafkaSource(odsTopic, groupId, kafkaServers),
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");
      //  sourceStream.print();

        SingleOutputStreamOperator<String> orderStream = sourceStream.map(JSON::parseObject)
                .filter(json -> "order_info".equals(json.getJSONObject("source").getString("table")) && "r".equals(json.getString("op")))
                .map(json -> json.getJSONObject("after").toJSONString());

        SingleOutputStreamOperator<String> refundStream = sourceStream.map(JSON::parseObject)
                .filter(json -> "refund_info".equals(json.getJSONObject("source").getString("table")) && "r".equals(json.getString("op")))
                .map(json -> json.getJSONObject("after").toJSONString());

        SingleOutputStreamOperator<String> behaviorStream  = sourceStream.map(JSON::parseObject)
                .filter(json -> "user_behavior".equals(json.getJSONObject("source").getString("table")) && "r".equals(json.getString("op")))
                .map(json -> json.getJSONObject("after").toJSONString());

        orderStream.print("order"+"->");
        refundStream.print("refund"+"->");
        behaviorStream.print("behavior"+"->");

        orderStream.sinkTo(KafkaUtils.buildKafkaSink(kafkaServers, "dwd_order_info_gongdan"));
        refundStream.sinkTo(KafkaUtils.buildKafkaSink(kafkaServers, "dwd_refund_info_gongdan"));
        behaviorStream.sinkTo(KafkaUtils.buildKafkaSink(kafkaServers, "dwd_user_behavior_gongdan"));


        env.execute("OdsToDwdFactStreamJob");
    }
}
