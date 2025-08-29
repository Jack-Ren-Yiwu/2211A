package com.app.ads;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class LimitProcessFunction<T> extends KeyedProcessFunction<Boolean, T, T> {

    private final long maxCount;

    public LimitProcessFunction(long maxCount) {
        this.maxCount = maxCount;
    }

    private transient ValueState<Long> counter;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        counter = getRuntimeContext().getState(
                new ValueStateDescriptor<>("recordCounter", Long.class)
        );
    }

    @Override
    public void processElement(T value, Context ctx, Collector<T> out) throws Exception {
        Long count = counter.value();
        if (count == null) count = 0L;
        if (count < maxCount) {
            out.collect(value);
            counter.update(count + 1);
        }
    }
}
