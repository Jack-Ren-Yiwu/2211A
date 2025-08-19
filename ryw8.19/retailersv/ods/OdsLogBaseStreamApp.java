package com.retailersv.ods;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OdsLogBaseStreamApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaUtils.getKafkaSource("realtime_log", "ods_log_group", "cdh01:9092");

        SingleOutputStreamOperator<JSONObject> jsonStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka_Source")
                .map(JSON::parseObject)
                .name("Parse_JSON");

        // 4. 拆分各类日志流
        SingleOutputStreamOperator<String> startLogStream = jsonStream
                .filter(json -> json.containsKey("start"))
                .map(JSON::toJSONString)
                .name("Start_Log");

        SingleOutputStreamOperator<String> pageLogStream = jsonStream
                .filter(json -> json.containsKey("page"))
                .map(JSON::toJSONString)
                .name("Page_Log");

        SingleOutputStreamOperator<String> displayLogStream = jsonStream
                .filter(json -> json.containsKey("displays"))
                .map(JSON::toJSONString)
                .name("Display_Log");

        SingleOutputStreamOperator<String> actionLogStream = jsonStream
                .filter(json -> json.containsKey("actions"))
                .map(JSON::toJSONString)
                .name("Action_Log");

        SingleOutputStreamOperator<String> errorLogStream = jsonStream
                .filter(json -> json.containsKey("err"))
                .map(JSON::toJSONString)
                .name("Error_Log");

        // 5. 写入 Kafka ODS topic
        startLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "ods_start_log"));
        pageLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "ods_page_log"));
        displayLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "ods_display_log"));
        actionLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "ods_action_log"));
        errorLogStream.sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "ods_error_log"));



        env.execute();
    }
}
