package com.retailersv.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DwdOrderDetailEnrichJob {

    private static final OutputTag<JSONObject> dirtyTag = new OutputTag<JSONObject>("dirty-data") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-dwd");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setTopics("dwd_order_detail_wide_kafka")
                .setGroupId("flink_order_detail_enrich")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SafeStringSchema())
                .build();

        SingleOutputStreamOperator<JSONObject> stream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source")
                .filter(line -> line != null && !line.trim().isEmpty())
                .process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<JSONObject> out) {
                        try {
                            JSONObject json = JSONObject.parseObject(value);
                            if (json == null || json.isEmpty()) {
                                throw new RuntimeException("Empty JSON");
                            }
                            out.collect(json);
                        } catch (Exception e) {
                            JSONObject dirty = new JSONObject();
                            dirty.put("raw", value);
                            dirty.put("error", "JSON解析失败: " + e.getMessage());
                            ctx.output(dirtyTag, dirty);
                        }
                    }
                });

        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_sku_info", "sku_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_spu_info", "spu_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_category3", "category3_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_trademark", "tm_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_user_info", "user_id");
        stream = (SingleOutputStreamOperator<JSONObject>) asyncJoin(stream, "dim_base_province", "province_id");

        stream = stream.map(DwdOrderDetailEnrichJob::normalizeFields);

        stream.print("enriched");

        DataStream<JSONObject> filtered = stream
                .filter(json -> json.getString("order_id") != null);

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dwd_order_detail_enriched")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setKafkaProducerConfig(new Properties() {{
                    put(ProducerConfig.ACKS_CONFIG, "all");
                }})
                .build();

        filtered.map(JSON::toJSONString).sinkTo(kafkaSink);

        stream.getSideOutput(dirtyTag).print("dirty");

        env.execute("DwdOrderDetailEnrichJob");
    }

    public static class SafeStringSchema implements DeserializationSchema<String> {
        @Override
        public String deserialize(byte[] message) {
            return message == null ? null : new String(message);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    private static JSONObject normalizeFields(JSONObject json) {
        try {
            if (json.containsKey("order_price")) {
                json.put("order_amount", Double.parseDouble(json.getString("order_price")));
            }
            if (json.containsKey("pay_amount")) {
                json.put("payment_amount", Double.parseDouble(json.getString("pay_amount")));
            }
        } catch (Exception e) {
            json.put("cast_error", e.getMessage());
        }
        return json;
    }

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
                            String key = input.getString(joinKey);
                            if (key == null || key.trim().isEmpty()) {
                                resultFuture.complete(Collections.singleton(input));
                                return;
                            }

                            CompletableFuture
                                    .supplyAsync(() -> {
                                        try {
                                            Get get = new Get(Bytes.toBytes(key));
                                            Result result = table.get(get);
                                            if (!result.isEmpty()) {
                                                JSONObject dimJson = new JSONObject();
                                                result.listCells().forEach(cell -> {
                                                    String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                                                    String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                                    if (col != null && val != null) {
                                                        dimJson.put("dim_" + col, val);
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
