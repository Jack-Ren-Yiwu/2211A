package app.ods;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import util.MyKafkaUtil;

public class ODS_readkfk_flatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> medical = env.addSource(MyKafkaUtil.getKafkaConsumer("flink_medical").setStartFromEarliest());
        SingleOutputStreamOperator<JSONObject> flatMap = medical.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    if (jsonObject != null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println("脏数据");
                }

            }
        });

        //flatMap.print();
        flatMap.map(JSONObject::toString).addSink(MyKafkaUtil.getKafkaProducer("medical_ods_base_topic"));

        env.execute();
    }
}
