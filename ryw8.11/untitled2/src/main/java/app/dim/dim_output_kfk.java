package app.dim;

import app.common.MedicalCommon;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import util.CreateEnvutil;
import util.KafkaUtil;
import util.MyKafkaUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class dim_output_kfk {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = CreateEnvutil.getStreamEnv(8081, "dim_output_kfk");

        env.setParallelism(1);

        KafkaSource<String> kafkaConsumer = KafkaUtil.getKafkaConsumer("medical_ods_base_topic", "dim_output_kfk");

        DataStreamSource<String> source = (DataStreamSource<String>) env.fromSource(kafkaConsumer, WatermarkStrategy.noWatermarks(), "medical_ods_base_topic")
                .uid("medical_ods_base_topic");

        source.print();
       // env.fromSource()

       // DataStreamSource<String> medical = env.addSource(MyKafkaUtil.getKafkaConsumer("medical_ods_base_topic").setStartFromEarliest());


        env.execute();
    }
}
