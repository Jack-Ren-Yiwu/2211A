package app.ods;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.FlinkCDC;
import util.MyKafkaUtil;


public class ODS_flinkCDC_kfk {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启 checkpoint，保证一致性
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.setParallelism(1);

//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("cdh01")
//                .port(3306)
//                .username("root")
//                .password("123456")
//                .databaseList("medical")
//                .tableList("medical.*")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                //是否需要先读一份完整的数据
//                .startupOptions(StartupOptions.initial())
//                .build();
//
//        DataStreamSource<String> flinkCDC = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "flinkCDC");
//
//        flinkCDC.print();

        SingleOutputStreamOperator<String> medical = FlinkCDC.mysqlCDC(env, "medical", "*");
        medical.print();
        medical.addSink(MyKafkaUtil.getKafkaProducer("flink_medical"));

        env.execute();

    }
}
