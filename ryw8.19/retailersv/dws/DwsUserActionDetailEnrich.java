package com.retailersv.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DwsUserActionDetailEnrich {
    public static void main(String[] args) throws Exception {
        // 1. Flink 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-dws");

        // 2. Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("dws_user_action_detail")
                .setGroupId("flink_user_action_enrich")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. 数据清洗和脏数据处理
        OutputTag<JSONObject> dirtyTag = new OutputTag<JSONObject>("dirty") {};
        SingleOutputStreamOperator<JSONObject> stream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject json = JSONObject.parseObject(value);
                            out.collect(json);
                        } catch (Exception e) {
                            JSONObject dirty = new JSONObject();
                            dirty.put("raw", value);
                            dirty.put("err", e.getMessage());
                            ctx.output(dirtyTag, dirty);
                        }
                    }
                });

        // 4. 异步关联多个维度
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_sku_info", "sku_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_trademark", "tm_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_category3", "category3_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_user_info", "uid");

        stream.print();
        // 5. 输出主流到 Kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dws_user_action_enriched")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        stream.map(JSON::toJSONString).sinkTo(kafkaSink);

        // 6. 输出脏数据
        stream.getSideOutput(dirtyTag).print("dirty");

        env.execute("DwsUserActionDetailEnrich");
    }

    /**
     * 通用异步维度拉宽方法
     */
    private static DataStream<JSONObject> asyncJoin(DataStream<JSONObject> input, String tableName, String joinKey) {
        return AsyncDataStream.unorderedWait(
                input,
                new RichAsyncFunction<JSONObject, JSONObject>() {
                    private transient Connection conn;
                    private transient Table table;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        conn = HbaseUtils.getConnection();
                        table = conn.getTable(TableName.valueOf(tableName));
                    }

                    @Override
                    public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
                        try {
                            String key = null;
                            // 从顶层拿
                            if (input.containsKey(joinKey)) {
                                key = input.getString(joinKey);
                            }
                            // 从 common 字段拿
                            else if (input.containsKey("common")) {
                                key = input.getJSONObject("common").getString(joinKey);
                            }
                            // 从 display 字段拿
                            else if (input.containsKey("display")) {
                                key = input.getJSONObject("display").getString("item");
                            }

                            if (key == null || key.trim().isEmpty()) {
                                resultFuture.complete(Collections.singleton(input));
                                return;
                            }

                            final String finalKey = key;

                            CompletableFuture
                                    .supplyAsync(() -> {
                                        try {
                                            Get get = new Get(Bytes.toBytes(finalKey));
                                            Result result = table.get(get);
                                            if (!result.isEmpty()) {
                                                JSONObject dimJson = new JSONObject();
                                                result.listCells().forEach(cell -> {
                                                    String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                                                    String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                                    if (col != null && val != null) {
                                                        dimJson.put(col, val);
                                                    }
                                                });
                                                input.putAll(dimJson);
                                            }
                                        } catch (Exception e) {
                                            input.put(tableName + "_error", e.getMessage());
                                        }
                                        return input;
                                    })
                                    .thenAccept(res -> resultFuture.complete(Collections.singleton(res)));

                        } catch (Exception e) {
                            input.put(tableName + "_exception", e.getMessage());
                            resultFuture.complete(Collections.singleton(input));
                        }
                    }


                    @Override
                    public void close() throws Exception {
                        if (table != null) table.close();
                        if (conn != null) conn.close();
                    }
                },
                60, TimeUnit.SECONDS, 100);
    }
}

