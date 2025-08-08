package org.example;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Time : 2022/11/13 17:37
 * @Author :tzh
 * @File : FlinkCDC
 * @Project : gmall_flink
 * <p>
 * <p>
 * maxwell 和 flinkCDC
 * 相同 1.都是去监控mysql的binlog  （row 、statement、mixed） row 将每行数据最新的状态写入binlog中
 * 2.都去读binlog
 * 3.都支持初始化（全量）
 * 4.都支持 增量
 * 5. 多库多表
 * 不同之处：
 * 1. maxwell的数据格式都是json
 * 2.cdc数据格式可以自定义（常用的是json）
 * 3.maxwell 不支持高可用
 * 4.cdc 集群 高可用
 * <p>
 * FlinkCDC如何实现增量和全量采集
 * 全量：直接读表数据 类似select * from table;
 * 增量：读binlog日志
 **/
public class FlinkCDC {

    public static SingleOutputStreamOperator<String>  mysqlCDC(StreamExecutionEnvironment env,String database, String... tables) throws Exception {
        //1.获取执行环境


        //1.1 设置CK&状态后端 ,如下代码自己做测试不需要不然每次都要打开HDFS，未来在生产环境中写代码时这些东西一定是要有的
        //env.setStateBackend(new FsStateBackend("hdfs://node101:8020/gmall-flink-2021/ck"));
        //开启 Checkpoint,每隔 5 秒钟做一次 CK  头和头
        //env.enableCheckpointing(5000L);
        //指定 CK 的一致性语义
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        //env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);//尾和头

        String tableList = "";
        for (String table : tables) {
            tableList += database + "." + table + ",";
        }

        //2.通过FlinkCDC 构建SourceFunction并读取数据
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("cdh01")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList(database)//可以同时读多个库
                .tableList(tableList.substring(0, tableList.length() - 1))
                //可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
                //.tableList("gmall-2021-flink.base_trademark")
                //.deserializer(new StringDebeziumDeserializationSchema())
                .deserializer( new  JsonDebeziumDeserializationSchema())
                //initial 初始化，
                //.startupOptions(StartupOptions.initial())
                .startupOptions(StartupOptions.initial())  //latest 是直接读最新的数据，而不会去打印老的数据
                .build();
        SingleOutputStreamOperator<String> streamSource = env.addSource(sourceFunction);
                        /*.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return JSONObject.parseObject(value).getJSONObject("after").toJSONString();
            }
        })*/;
        //3.打印数据并将数据写入Kafka
        streamSource.print();
        //4.启动任务
        return streamSource;
    }

}
