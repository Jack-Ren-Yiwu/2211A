package com.app;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafKaUtils;
import com.util.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CDC_mysql_to_kfk {
    private static final String CDH_ZOOKEEPER_SERVER = ConfigUtils.getString("zookeeper.server.host.list");
    private static final String CDH_KAFKA_SERVER = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");
    private static final String MYSQL_CDC_TO_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        MySqlSource<String> mySQLCdcSource = CdcSourceUtils.getMySQLCdcSource("root", "123456", "test", ".*", StartupOptions.initial());
//        env.fromSource(mySQLCdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                .print();

        MySqlSource<String> mySQLCdcSource = CdcSourceUtils.getMySQLCdcSource(ConfigUtils.getString("mysql.database"), ".*",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pass"),
                StartupOptions.initial());

        MySqlSource<String> mySQLCdcDimConfSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.databases.conf"),
                "realtime_v1_config.table_process_dim",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );

        DataStreamSource<String> cdcmainStream = env.fromSource(mySQLCdcSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        DataStreamSource<String> cdcdimStream = env.fromSource(mySQLCdcDimConfSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        SingleOutputStreamOperator<JSONObject> cdcmainStreamMap = cdcmainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json").setParallelism(1);


        cdcmainStreamMap.map(JSONObject::toString).sinkTo(
                KafKaUtils.buildKafkaSink(CDH_KAFKA_SERVER, MYSQL_CDC_TO_KAFKA_TOPIC)
        ).uid("mysql_cdc_to_kafka_topic").name("mysql_cdc_to_kafka_topic");
        cdcmainStreamMap.print("cdcDbMainStreamMap");

        cdcdimStream.map(JSONObject::parseObject)
                        .uid("dim_data_convert_json").name("dim_data_convert_json").setParallelism(1);



        env.execute();
    }
}
