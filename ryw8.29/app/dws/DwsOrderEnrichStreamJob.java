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
/**
 * DwsOrderEnrichStreamJob
 * 作用：从 Kafka 的 dwd_order_wide_gongdan 读取订单宽表数据，
 *       通过异步 IO 查询 HBase 维度表，丰富订单数据，
 *       最终写入 Kafka 的 dws_order_enriched_gongdan 主题。
 */
public class DwsOrderEnrichStreamJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3); // 设置并行度为 3

        // 2. 从 Kafka 读取 DWD 宽表数据
        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.getKafkaSource("dwd_order_wide_gongdan", "dws_order_enrich_group", "cdh01:9092"),
                WatermarkStrategy.noWatermarks(), // 不处理事件时间，纯 enrich 逻辑
                "KafkaSource-dwd_order_wide_gongdan" // Source 名称
        );

        // 3. 解析 JSON
        SingleOutputStreamOperator<JSONObject> parsed = source.map(JSON::parseObject);

        // 4. 异步维度 enrich（异步 I/O 查询 HBase 维度表）
        SingleOutputStreamOperator<JSONObject> enriched = AsyncDataStream.unorderedWait(
                parsed, // 输入数据流
                new OrderAsyncDimEnrichFunction(), // 自定义异步函数
                60, // 超时时间
                TimeUnit.SECONDS,
                100 // 最大并发请求数
        );

        // 5. 调试输出
        enriched.print();

        // 6. 将 enrich 后的数据写回 Kafka 的 dws_order_enriched_gongdan 主题
        enriched
                .map(JSON::toJSONString) // 转成字符串
                .sinkTo(KafkaUtils.buildKafkaSink("cdh01:9092", "dws_order_enriched_gongdan"));

        // 7. 启动作业
        env.execute("DwsOrderEnrichStreamJob");
    }
    /**
     * 自定义异步维度 enrich 函数
     * 作用：从 HBase 维度表查询数据，并 enrich 到订单宽表 JSON 中
     */
    public static class OrderAsyncDimEnrichFunction extends RichAsyncFunction<JSONObject, JSONObject> {

        private static final Logger log = LoggerFactory.getLogger(DwsOrderEnrichStreamJob.class);

        private transient Connection hbaseConn; // HBase 连接
        private transient Cache<String, JSONObject> dimCache; // 本地缓存，用于减少 HBase 压力

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化 HBase 连接
            org.apache.hadoop.conf.Configuration hConf = HBaseConfiguration.create();
            hConf.set("hbase.zookeeper.quorum", "cdh01:2181");
            hbaseConn = ConnectionFactory.createConnection(hConf);

            // 初始化缓存（最大 10000 条，10 分钟过期）
            dimCache = CacheBuilder.newBuilder()
                    .maximumSize(10000)
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    .build();
        }

        @Override
        public void asyncInvoke(JSONObject input, ResultFuture<JSONObject> resultFuture) {
            // 提取 key 字段
            String userId = input.getString("user_id");
            String skuId = input.getString("sku_id");
            String category3Id = input.getString("category3_id");
            String province = input.getString("province");
            String pageId = input.getString("page_id");

            // 并行查询多个维度表
            CompletableFuture
                    .allOf(
                            queryAndFill(input, "dim_user_info_1", "user_id", userId),
                            queryAndFill(input, "dim_sku_info_1", "sku_id", skuId),
                            queryAndFill(input, "dim_category_1", "category3_id", category3Id),
                            queryAndFill(input, "dim_province_1", "province", province),
                            queryAndFill(input, "dim_platform_page_type_1", "page_id", pageId)
                    )
                    .thenAccept(v -> resultFuture.complete(Collections.singleton(input))); // enrich 完成后返回
        }

        @Override
        public void timeout(JSONObject input, ResultFuture<JSONObject> resultFuture) {
            // 超时日志 + 返回原始数据，保证流程不中断
            log.warn("Async timeout. input: {}", input.toJSONString());
            resultFuture.complete(Collections.singleton(input));
        }

        /**
         * 查询 HBase 维度表并填充数据
         */
        private CompletableFuture<Void> queryAndFill(JSONObject input, String table, String keyField, String keyVal) {
            return CompletableFuture.runAsync(() -> {
                if (keyVal == null || keyVal.trim().isEmpty()) return;

                try {
                    String cacheKey = table + ":" + keyVal;
                    JSONObject dim = dimCache.getIfPresent(cacheKey); // 先查本地缓存

                    if (dim == null) {
                        // 缓存没有则查 HBase
                        Table hTable = hbaseConn.getTable(TableName.valueOf("default:" + table));
                        Get get = new Get(Bytes.toBytes(keyVal));
                        Result result = hTable.get(get);

                        if (!result.isEmpty()) {
                            dim = new JSONObject();
                            for (Cell cell : result.rawCells()) {
                                String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                                String val = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                dim.put(col, val); // 将维度字段放入 JSON
                            }
                            dimCache.put(cacheKey, dim); // 缓存
                        }
                        hTable.close();
                    }

                    // enrich 维度字段到主流 JSON
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
            if (hbaseConn != null) hbaseConn.close(); // 关闭 HBase 连接
        }
    }
}
