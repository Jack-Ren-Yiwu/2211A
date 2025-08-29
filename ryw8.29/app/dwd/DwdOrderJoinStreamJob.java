package com.app.dwd;
// 包名：存放在 dwd 层相关任务下

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
// 引入所需的依赖类，包含 Kafka 工具类、Flink API、时间窗口工具和 JSON 解析工具

/**
 * DWD 层订单流与退款流 Join 任务
 */
public class DwdOrderJoinStreamJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);  // 设置并行度为 3

        String kafkaServers = "cdh01:9092"; // Kafka 集群地址

        // 2. 读取订单流（来自 Kafka 主题 dwd_order_info_gongdan）
        DataStream<OrderInfo> orderStream = env
                .fromSource(
                        KafkaUtils.getKafkaSource("dwd_order_info_gongdan", "order_join_group", kafkaServers),
                        // 使用 KafkaSource 读取订单主题，消费组为 order_join_group

                        WatermarkStrategy
                                .<String>forMonotonousTimestamps() // 初始定义一个单调递增的时间戳策略（仅占位）
                                .withTimestampAssigner((event, ts) ->
                                        JSON.parseObject(event).getString("order_time").hashCode()), // 简化模拟，先用 hashCode 代替时间
                        "order_info") // source 名称
                .map(json -> JSON.parseObject(json, OrderInfo.class)) // 将 JSON 解析为 OrderInfo 对象
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许乱序 5 秒
                                .withTimestampAssigner((e, ts) -> e.getOrderTimeMillis()) // 提取 order_time 字段作为事件时间
                );

        // 3. 读取退款流（来自 Kafka 主题 dwd_refund_info_gongdan）
        DataStream<RefundInfo> refundStream = env
                .fromSource(
                        KafkaUtils.getKafkaSource("dwd_refund_info_gongdan", "order_join_group", kafkaServers),
                        // 使用 KafkaSource 读取退款主题

                        WatermarkStrategy.noWatermarks(), // 初始不设置 Watermark
                        "refund_info") // source 名称
                .map(json -> JSON.parseObject(json, RefundInfo.class)) // 将 JSON 解析为 RefundInfo 对象
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RefundInfo>forBoundedOutOfOrderness(Duration.ofSeconds(5)) // 允许乱序 5 秒
                                .withTimestampAssigner((e, ts) -> e.getRefundTimeMillis()) // 提取 refund_time 字段作为事件时间
                );

        // 4. 基于 order_id 做 interval join（订单流与退款流关联）
        SingleOutputStreamOperator<OrderWide> joinedStream = orderStream
                .keyBy(OrderInfo::getOrder_id) // 订单流按 order_id 分组
                .intervalJoin(refundStream.keyBy(RefundInfo::getOrder_id)) // 与退款流按 order_id 进行 interval join
                .between(Time.minutes(-5), Time.minutes(5)) // Join 条件：退款时间在订单时间的前后 5 分钟范围
                .process(new ProcessJoinFunction<OrderInfo, RefundInfo, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo order,
                                               RefundInfo refund,
                                               Context ctx,
                                               Collector<OrderWide> out) {
                        // 处理符合条件的订单 + 退款匹配
                        OrderWide wide = new OrderWide(order, refund); // 封装成宽表对象
                        out.collect(wide); // 输出结果
                    }
                });

        joinedStream.print(); // 打印到控制台，方便调试

        // 5. 将 Join 后的宽表数据写入 Kafka 主题 dwd_order_wide_gongdan
        joinedStream
                .map(JSON::toJSONString) // 将宽表对象转为 JSON 字符串
                .sinkTo(KafkaUtils.buildKafkaSink(kafkaServers, "dwd_order_wide_gongdan")); // 输出到 Kafka sink

        env.execute("DwdOrderJoinStreamJob"); // 启动作业，名称为 DwdOrderJoinStreamJob
    }
}
