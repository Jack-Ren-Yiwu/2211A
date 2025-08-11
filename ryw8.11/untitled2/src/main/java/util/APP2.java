package util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public class APP2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从 Kafka 读取
        DataStreamSource<String> medical = env.addSource(
                MyKafkaUtil.getKafkaConsumer("medical").setStartFromEarliest()
        );

        // 2. 解析 Kafka CDC JSON，提取 after 并加 tableName
        SingleOutputStreamOperator<JSONObject> parseStream = medical.map(json -> {
            JSONObject jsonObject = JSON.parseObject(json);
            JSONObject after = jsonObject.getJSONObject("after");

            if (after != null) {
                // 从 Debezium 格式 source.table 取表名
                String tableName = jsonObject.getJSONObject("source").getString("table");
                after.put("tableName", tableName); // ✅ 保证字段名一致
            }
            return after;
        }).filter(Objects::nonNull);

        // 3. 分流，统一用 tableName 判断
        SingleOutputStreamOperator<JSONObject> process = parseStream.process(
                new ProcessFunction<JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> collector) {
                        String tableName = jsonObject.getString("tableName");
                        if ("hospital".equals(tableName)) {
                            context.output(HOSPITAL_TAG, jsonObject);
                        } else if ("patient".equals(tableName)) {
                            context.output(PATIENT_TAG, jsonObject);
                        } else if ("payment".equals(tableName)) {
                            context.output(PAYMENT_TAG, jsonObject);
                        } else {
                            collector.collect(jsonObject);
                        }
                    }
                }
        );

        // 4. 获取侧流数据
        DataStream<JSONObject> hospitalOutPut = process.getSideOutput(HOSPITAL_TAG);
        DataStream<JSONObject> patientOutPut = process.getSideOutput(PATIENT_TAG);
        DataStream<JSONObject> paymentOutPut = process.getSideOutput(PAYMENT_TAG);

        // 打印检查
        hospitalOutPut.print("hospitalOutPut");
        patientOutPut.print("patientOutPut");
        paymentOutPut.print("paymentOutPut");

        // 5. 写 HBase（Sink 里会用 tableName）
        hospitalOutPut.addSink(new HBaseSinkFunction());
        patientOutPut.addSink(new HBaseSinkFunction());
        paymentOutPut.addSink(new HBaseSinkFunction());

        env.execute();
    }

    public static final OutputTag<JSONObject> HOSPITAL_TAG =
            new OutputTag<JSONObject>("hospital") {};
    public static final OutputTag<JSONObject> PATIENT_TAG =
            new OutputTag<JSONObject>("patient") {};
    public static final OutputTag<JSONObject> PAYMENT_TAG =
            new OutputTag<JSONObject>("payment") {};
}
