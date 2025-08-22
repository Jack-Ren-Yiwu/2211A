package com.retailersv.alert;

import com.retailersv.bean.EnrichedStats;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class AlertEvaluatorFunction extends BroadcastProcessFunction<EnrichedStats, AlertRule, Tuple2<String, String>> {

    private final MapStateDescriptor<String, AlertRule> ruleStateDescriptor;

    public AlertEvaluatorFunction(MapStateDescriptor<String, AlertRule> ruleStateDescriptor) {
        this.ruleStateDescriptor = ruleStateDescriptor;
    }

    @Override
    public void processElement(EnrichedStats stat, ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
        for (Map.Entry<String, AlertRule> entry : ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
            AlertRule rule = entry.getValue();
            if (rule.getStatus() != 1) continue;

            String field = rule.getField_name();
            String op = rule.getOperator();
            Double threshold = rule.getThreshold();

            Double value = null;
            switch (field) {
                case "order_amount":
                    value = stat.getOrder_amount();
                    break;
                case "refund_amount":
                    value = stat.getRefund_amount();
                    break;
                case "refund_rate":
                    if (stat.getOrder_amount() != null && stat.getOrder_amount() > 0) {
                        value = stat.getRefund_amount() / stat.getOrder_amount();
                    }
                    break;
                case "sku_num":
                    value = stat.getSku_num() != null ? stat.getSku_num().doubleValue() : null;
                    break;
                case "order_ct":
                    value = stat.getOrder_ct() != null ? stat.getOrder_ct().doubleValue() : null;
                    break;
                case "order_user_ct":
                    value = stat.getOrder_user_ct() != null ? stat.getOrder_user_ct().doubleValue() : null;
                    break;
                case "refund_user_ct":
                    value = stat.getRefund_user_ct() != null ? stat.getRefund_user_ct().doubleValue() : null;
                    break;
                default:
                    break;
            }

            if (value != null && compare(value, threshold, op)) {
                out.collect(Tuple2.of(rule.getDescription(), formatAlert(stat)));
            }
        }
    }

    private boolean compare(Double val, Double threshold, String op) {
        switch (op) {
            case ">": return val > threshold;
            case ">=": return val >= threshold;
            case "<": return val < threshold;
            case "<=": return val <= threshold;
            case "==": return val.equals(threshold);
            default: return false;
        }
    }

    @Override
    public void processBroadcastElement(AlertRule rule, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
        BroadcastState<String, AlertRule> ruleState = ctx.getBroadcastState(ruleStateDescriptor);
        ruleState.put(rule.getField_name(), rule);
    }


    private String formatAlert(EnrichedStats stat) {
        return String.format("ALERT SKU: %s | order_amount: %.2f | refund_amount: %.2f",
                stat.getSku_id(), stat.getOrder_amount(), stat.getRefund_amount());
    }
}