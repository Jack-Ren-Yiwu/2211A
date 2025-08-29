package com.app.dwd;
// 包名：dwd 层相关任务

import com.alibaba.fastjson.JSON;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
// 导入所需的 Flink API 和工具类

/**
 * OdsToDwdFactStreamJob
 * 作用：从 ODS 层 Kafka 主题读取数据（包含多张表的变更日志），
 *       按照表名拆分不同事实表流（订单、退款、用户行为），
 *       再写入到 DWD 层对应的 Kafka 主题。
 */
public class OdsToDwdFactStreamJob {
    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);  // 设置并行度为 3

        // 2. 定义 Kafka 参数
        String kafkaServers = "cdh01:9092";  // Kafka 集群地址
        String odsTopic = "ods_gongdan07_order"; // ODS 层主题（包含所有表数据）
        String groupId = "ods_to_dwd_group"; // 消费组 ID

        // 3. 从 Kafka ODS 主题读取原始数据流
        DataStreamSource<String> sourceStream = env.fromSource(
                KafkaUtils.getKafkaSource(odsTopic, groupId, kafkaServers), // 使用工具类获取 KafkaSource
                WatermarkStrategy.noWatermarks(), // 不设置事件时间和水位线，仅做事实表拆分
                "Kafka Source"); // Source 名称

        // 4. 拆分订单流（order_info 表）
        SingleOutputStreamOperator<String> orderStream = sourceStream
                .map(JSON::parseObject) // 将 JSON 字符串解析为 JSONObject
                .filter(json -> "order_info".equals(json.getJSONObject("source").getString("table")) // 表名 = order_info
                        && "r".equals(json.getString("op"))) // 操作类型 = r（表示读取 after 数据）
                .map(json -> json.getJSONObject("after").toJSONString()); // 提取 after 部分作为订单事实数据

        // 5. 拆分退款流（refund_info 表）
        SingleOutputStreamOperator<String> refundStream = sourceStream
                .map(JSON::parseObject) // 解析 JSON
                .filter(json -> "refund_info".equals(json.getJSONObject("source").getString("table")) // 表名 = refund_info
                        && "r".equals(json.getString("op"))) // 操作类型 = r
                .map(json -> json.getJSONObject("after").toJSONString()); // 提取 after 字段

        // 6. 拆分用户行为流（user_behavior 表）
        SingleOutputStreamOperator<String> behaviorStream  = sourceStream
                .map(JSON::parseObject) // 解析 JSON
                .filter(json -> "user_behavior".equals(json.getJSONObject("source").getString("table")) // 表名 = user_behavior
                        && "r".equals(json.getString("op"))) // 操作类型 = r
                .map(json -> json.getJSONObject("after").toJSONString()); // 提取 after 字段

        // 7. 调试打印，验证拆分结果
        orderStream.print("order ->");       // 打印订单流
        refundStream.print("refund ->");     // 打印退款流
        behaviorStream.print("behavior ->"); // 打印用户行为流

        // 8. 将拆分后的流写入 DWD 层 Kafka 主题
        orderStream.sinkTo(KafkaUtils.buildKafkaSink(kafkaServers, "dwd_order_info_gongdan"));
        // 输出到 dwd_order_info_gongdan 主题

        refundStream.sinkTo(KafkaUtils.buildKafkaSink(kafkaServers, "dwd_refund_info_gongdan"));
        // 输出到 dwd_refund_info_gongdan 主题

        behaviorStream.sinkTo(KafkaUtils.buildKafkaSink(kafkaServers, "dwd_user_behavior_gongdan"));
        // 输出到 dwd_user_behavior_gongdan 主题

        // 9. 启动作业
        env.execute("OdsToDwdFactStreamJob"); // 任务名称：OdsToDwdFactStreamJob
    }
}
