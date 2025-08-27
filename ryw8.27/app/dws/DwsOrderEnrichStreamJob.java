package com.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DwsOrderEnrichStreamJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_order_wide_gongdan", "dws_order_enrich_group", "cdh01:9092"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource-dwd_order_wide_gongdan"
        );

        SingleOutputStreamOperator<JSONObject> parsed = source.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> enriched = AsyncDataStream.unorderedWait(
                parsed,
                new OrderAsyncDimEnrichFunction(),
                60,
                TimeUnit.SECONDS,
                100
        );

        enriched.print();
        enriched
                .map(JSON::toJSONString)
                .sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "dws_order_enriched_gongdan"));

        env.execute("DwsOrderEnrichStreamJob");
    }

    public static class OrderAsyncDimEnrichFunction extends RichAsyncFunction<JSONObject, JSONObject> {

        private static final Logger log = LoggerFactory.getLogger(DwsOrderEnrichStreamJob.class);
        private transient Connection hbaseConn;
        private transient Cache<String, JSONObject> dimCache;

        @Override
        public void open(Configuration parameters) throws Exception {
            org.apache.hadoop.conf.Configuration hConf = HBaseConfiguration.create();
            hConf.set("hbase.zookeeper.quorum", "cdh01:2181");
            hbaseConn = ConnectionFactory.createConnection(hConf);
            dimCache = CacheBuilder.newBuilder()
                    .maximumSize(10000)
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    .build();
        }

        @Override
        public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
            String userId = input.getString("user_id");
            String skuId = input.getString("sku_id");
            String category3Id = input.getString("category3_id");
            String province = input.getString("province");
            String pageId = input.getString("page_id");

            CompletableFuture
                    .allOf(
                            queryAndFill(input, "dim_user_info_1", "user_id", userId),
                            queryAndFill(input, "dim_sku_info_1", "sku_id", skuId),
                            queryAndFill(input, "dim_category_1", "category3_id", category3Id),
                            queryAndFill(input, "dim_province_1", "province", province),
                            queryAndFill(input, "dim_platform_page_type_1", "page_id", pageId)
                    )
                    .thenAccept(v -> resultFuture.complete(Collections.singleton(input)));
        }

        @Override
        public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) {
            log.warn("Async timeout. input: {}", input.toJSONString());
            resultFuture.complete(Collections.singleton(input)); // 继续处理
        }

        private CompletableFuture<Void> queryAndFill(JSONObject input, String table, String keyField, String keyVal) {
            return CompletableFuture.runAsync(() -> {
                if (keyVal == null || keyVal.trim().isEmpty()) return;

                try {
                    String cacheKey = table + ":" + keyVal;
                    JSONObject dim = dimCache.getIfPresent(cacheKey);

                    if (dim == null) {
                        Table hTable = hbaseConn.getTable(TableName.valueOf("default:" + table));
                        Get get = new Get(Bytes.toBytes(keyVal));
                        Result result = hTable.get(get);

                        if (!result.isEmpty()) {
                            dim = new JSONObject();
                            for (Cell cell : result.rawCells()) {
                                String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                                String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                dim.put(col, val);
                            }
                            dimCache.put(cacheKey, dim);
                        }
                        hTable.close();
                    }

                    if (dim != null) {
                        for (String k : dim.keySet()) {
                            input.put(keyField + "_" + k, dim.get(k));
                        }
                    }

                } catch (Exception e) {
                    log.error("HBase 查询失败：table={} keyField={} keyVal={}", table, keyField, keyVal, e);
                }
            });
        }

        @Override
        public void close() throws Exception {
            if (hbaseConn != null) hbaseConn.close();
        }
    }
}
