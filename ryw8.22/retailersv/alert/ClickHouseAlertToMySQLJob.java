package com.retailersv.alert;


import com.retailersv.bean.EnrichedStats;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.*;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ClickHouseAlertToMySQLJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 主数据流
        DataStream<EnrichedStats> mainStream = env.addSource(new ClickHouseEnrichedStatsSource());

        // 规则广播流
        MapStateDescriptor<String, AlertRule> ruleStateDescriptor =
                new MapStateDescriptor<>("alert-rules", String.class, AlertRule.class);

        BroadcastStream<AlertRule> ruleStream = env.addSource(new RuleBroadcastSource()).broadcast(ruleStateDescriptor);

        // 联合处理
        DataStream<Tuple2<String, String>> alertStream = mainStream
                .connect(ruleStream)
                .process(new AlertEvaluatorFunction(ruleStateDescriptor));

        // Sink
        alertStream.addSink(JdbcSink.sink(
                "INSERT INTO flink_wrong.alert_result (alert_type, alert_msg, create_time) VALUES (?, ?, ?)",
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setString(2, t.f1);
                    ps.setString(3, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                },
                JdbcExecutionOptions.builder().withBatchSize(5).withBatchIntervalMs(200).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://cdh01:3306/flink_wrong?useSSL=false")
                        .withUsername("root").withPassword("123456")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .build()
        ));

        env.execute("Flink 广播规则动态报警 Job");
    }
}
