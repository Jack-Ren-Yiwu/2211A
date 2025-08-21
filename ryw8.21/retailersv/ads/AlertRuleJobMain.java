package com.retailersv.ads;

import com.alibaba.fastjson.JSON;
import com.retailersv.bean.EnrichedStats;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class AlertRuleJobMain {
    public static void main(String[] args) throws Exception {
        // 1. 初始化环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 2. Kafka 配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "cdh01:9092");
        kafkaProps.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        kafkaProps.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        kafkaProps.setProperty(ProducerConfig.RETRIES_CONFIG, "3");

        // 3. 读取规则流（Kafka -> JSON -> AlertRule）
        DataStream<AlertRule> ruleStream = env
                .fromSource(KafkaSource.<String>builder()
                                .setBootstrapServers("cdh01:9092")
                                .setTopics("alert_rules_topic")
                                .setGroupId("alert-rule-group")
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .setStartingOffsets(OffsetsInitializer.latest())
                                .build(),
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "rule-source")
                .map(json -> JSON.parseObject(json, AlertRule.class));

        MapStateDescriptor<String, AlertRule> ruleStateDesc = new MapStateDescriptor<>(
                "alert_rules",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(AlertRule.class));

        BroadcastStream<AlertRule> broadcastRuleStream = ruleStream.broadcast(ruleStateDesc);

        // 4. 读取主业务流 enriched_trade_stats
        DataStream<EnrichedStats> mainStream = env
                .fromSource(KafkaSource.<String>builder()
                                .setBootstrapServers("cdh01:9092")
                                .setTopics("enriched_trade_stats_topic")
                                .setGroupId("alert-main-group")
                                .setValueOnlyDeserializer(new SimpleStringSchema())
                                .setStartingOffsets(OffsetsInitializer.latest())
                                .build(),
                        org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                        "data-source")
                .map(json -> JSON.parseObject(json, EnrichedStats.class))
                .keyBy(EnrichedStats::getSku_id);

        // 5. connect 主流 + 广播规则流
        BroadcastConnectedStream<EnrichedStats, AlertRule> connected = mainStream.connect(broadcastRuleStream);

        DataStream<String> alertStream = connected.process(
                new KeyedBroadcastProcessFunction<String, EnrichedStats, AlertRule, String>() {

                    private final MapStateDescriptor<String, AlertRule> ruleStateDescriptor =
                            new MapStateDescriptor<>("alert_rules",
                                    BasicTypeInfo.STRING_TYPE_INFO,
                                    TypeInformation.of(AlertRule.class));

                    @Override
                    public void processElement(EnrichedStats value,
                                               ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, AlertRule> rules = ctx.getBroadcastState(ruleStateDescriptor);
                        AlertRule rule = rules.get("refund_rate_alert");

                        if (rule != null && rule.isEnabled() && rule.getTable().equalsIgnoreCase("enriched_trade_stats")) {
                            double refundRate = value.getOrder_amount() != 0
                                    ? value.getRefund_amount() / value.getOrder_amount()
                                    : 0.0;

                            if (refundRate > rule.getThreshold()) {
                                out.collect(String.format(
                                        "退款率报警 SKU: %s 退款率: %.2f  阈值: %.2f",
                                        value.getSku_id(), refundRate, rule.getThreshold()));
                            }
                        }
                    }

                    @Override
                    public void processBroadcastElement(AlertRule rule,
                                                        Context ctx,
                                                        Collector<String> out) throws Exception {
                        BroadcastState<String, AlertRule> state = ctx.getBroadcastState(ruleStateDescriptor);
                        state.put(rule.getRuleId(), rule);
                        System.out.println("已更新规则: " + rule.getRuleId());
                    }
                }
        );

        // 6. 使用新版 KafkaSink 输出报警信息
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setKafkaProducerConfig(kafkaProps)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("alert_output_topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        alertStream.sinkTo(kafkaSink);

        env.execute("Flink 热更新报警任务 Job");
    }
}
