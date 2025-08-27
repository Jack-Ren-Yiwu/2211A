package com.app.dwd;

import com.alibaba.fastjson.JSON;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwdOrderJoinStreamJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        String kafkaServers = "cdh01:9092";

        // 2. 读取订单流
        DataStream<OrderInfo> orderStream = env
                .fromSource(KafkaUtils.getKafkaSource("dwd_order_info_gongdan", "order_join_group", kafkaServers),
                        WatermarkStrategy
                                .<String>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) ->
                                        JSON.parseObject(event).getString("order_time").hashCode()), // 简化模拟时间
                        "order_info")
                .map(json -> JSON.parseObject(json, OrderInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.getOrderTimeMillis())
                );

        // 3. 读取退款流
        DataStream<RefundInfo> refundStream = env
                .fromSource(KafkaUtils.getKafkaSource("dwd_refund_info_gongdan", "order_join_group", kafkaServers),
                        WatermarkStrategy.noWatermarks(),
                        "refund_info")
                .map(json -> JSON.parseObject(json, RefundInfo.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RefundInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((e, ts) -> e.getRefundTimeMillis())
                );

        // 4. 按 order_id 分组 + interval join
        SingleOutputStreamOperator<OrderWide> joinedStream = orderStream
                .keyBy(OrderInfo::getOrder_id)
                .intervalJoin(refundStream.keyBy(RefundInfo::getOrder_id))
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new ProcessJoinFunction<OrderInfo, RefundInfo, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo order,
                                               RefundInfo refund,
                                               Context ctx,
                                               Collector<OrderWide> out) {
                        OrderWide wide = new OrderWide(order, refund);
                        out.collect(wide);
                    }
                });

        joinedStream.print();
       //  5. 输出到 Kafka
        joinedStream
                .map(JSON::toJSONString)
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServers, "dwd_order_wide_gongdan"));

        env.execute("DwdOrderJoinStreamJob");
    }
}
