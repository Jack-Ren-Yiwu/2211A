package com.retailersv;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.HbaseUtils;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DwsTradeOrderWideAppMerged {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/tmp/flink-DWS");

        String kafkaServer = ConfigUtils.getString("kafka.bootstrap.servers");

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServer)
                .setTopics("dwd_fact_wide")
                .setGroupId("dws_trade_order_wide_group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        SingleOutputStreamOperator<OrderWide> orderWideStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "order_wide_source")
                .map(line -> {
                    JSONObject json = JSONObject.parseObject(line);
                    OrderWide ow = new OrderWide();
                    ow.setEventType(json.getString("eventType"));
                    ow.setOrderId(json.getString("orderId"));
                    ow.setUserId(json.getString("userId")); // may be null
                    ow.setSkuId(json.getString("skuId"));   // may be null
                    ow.setOrderAmount(json.getDouble("orderAmount"));
                    ow.setPayAmount(json.getDouble("payAmount"));
                    ow.setRefundAmount(json.getDouble("refundAmount"));
                    ow.setTs(json.getLong("ts"));
                    return ow;
                });

        SingleOutputStreamOperator<OrderWide> enriched = AsyncDataStream.unorderedWait(
                orderWideStream,
                new RichAsyncFunction<OrderWide, OrderWide>() {

                    private transient Connection conn;
                    private transient Table userTable;
                    private transient Table skuTable;

                    @Override
                    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                        conn = HbaseUtils.getConnection();
                        userTable = conn.getTable(TableName.valueOf("dim_user_info"));
                        skuTable = conn.getTable(TableName.valueOf("dim_sku_info"));
                    }

                    @Override
                    public void asyncInvoke(OrderWide input, ResultFuture<OrderWide> resultFuture) {
                        CompletableFuture<JSONObject> userFuture = CompletableFuture.supplyAsync(() -> {
                            try {
                                if (input.getUserId() == null) return new JSONObject();
                                Get get = new Get(Bytes.toBytes(input.getUserId()));
                                Result res = userTable.get(get);
                                return convertResultToJson(res);
                            } catch (Exception e) {
                                return new JSONObject();
                            }
                        });

                        CompletableFuture<JSONObject> skuFuture = CompletableFuture.supplyAsync(() -> {
                            try {
                                if (input.getSkuId() == null) return new JSONObject();
                                Get get = new Get(Bytes.toBytes(input.getSkuId()));
                                Result res = skuTable.get(get);
                                return convertResultToJson(res);
                            } catch (Exception e) {
                                return new JSONObject();
                            }
                        });

                        userFuture.thenCombine(skuFuture, (userInfo, skuInfo) -> {
                            if (userInfo != null) {
                                input.setUserGender(userInfo.getString("gender"));
                                input.setUserAge(userInfo.getString("age"));
                            }
                            if (skuInfo != null) {
                                input.setSkuName(skuInfo.getString("sku_name"));
                                input.setSpuId(skuInfo.getString("spu_id"));
                                input.setPrice(skuInfo.getString("price"));
                            }
                            return input;
                        }).thenAccept(result -> resultFuture.complete(Collections.singleton(result)));
                    }

                    private JSONObject convertResultToJson(Result result) {
                        JSONObject obj = new JSONObject();
                        if (result == null || result.isEmpty()) return obj;
                        result.listCells().forEach(cell -> {
                            String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                            String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                            obj.put(col, val);
                        });
                        return obj;
                    }

                    @Override
                    public void close() throws Exception {
                        if (userTable != null) userTable.close();
                        if (skuTable != null) skuTable.close();
                        if (conn != null) conn.close();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        enriched.print("enriched");

        env.execute("DwsTradeOrderWideAppMerged");
    }

    @Data
    public static class OrderWide implements Serializable {
        private String orderId;
        private String userId;
        private String skuId;
        private Double orderAmount;
        private Double payAmount;
        private Double refundAmount;
        private String eventType;
        private Long ts;

        private String userGender;
        private String userAge;
        private String skuName;
        private String spuId;
        private String price;
    }
}
