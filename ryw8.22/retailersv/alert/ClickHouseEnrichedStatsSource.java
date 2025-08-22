package com.retailersv.alert;

import com.retailersv.bean.EnrichedStats;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class ClickHouseEnrichedStatsSource implements SourceFunction<EnrichedStats> {
    private volatile boolean running = true;

    @Override
    public void run(SourceContext<EnrichedStats> ctx) throws Exception {
        while (running) {
            EnrichedStats mock = new EnrichedStats();
            mock.setSku_id("SKU-" + (int) (Math.random() * 10));
            mock.setOrder_amount(Math.random() < 0.3 ? 0 : Math.random() * 10000);
            mock.setRefund_amount(Math.random() * 2000);
            ctx.collect(mock);
            try {
                Thread.sleep(5000); // 每5秒采集一次
            } catch (InterruptedException e) {
                // 当线程被中断时，退出循环
                break;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
