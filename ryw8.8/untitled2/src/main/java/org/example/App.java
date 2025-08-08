package org.example;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Hello world!
 *
 */


public class App
{
    public static void main( String[] args ) throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 测试环境建议设置为1
        SingleOutputStreamOperator<String> test = FlinkCDC.mysqlCDC(env, "test", "*");
        //test.print();
        test.addSink(MyKafkaUtil.getKafkaProducer("test"));
        // 创建 CDC Source
//        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
//                .hostname("cdh01")
//                .port(3306)
//                .username("root")
//                .password("123456")
//                .databaseList("medical")
//                .tableList("medical.medicine")
//                .deserializer(new JsonDebeziumDeserializationSchema()) // 使用 JSON 反序列化器
//                .build();

        // 添加 Source 到流中
//        DataStreamSource<String> stream = env.addSource(sourceFunction);


        // 打印采集结果
//        stream.print();

        // 启动任务
        env.execute("MySQL CDC To Console");
    }
}
