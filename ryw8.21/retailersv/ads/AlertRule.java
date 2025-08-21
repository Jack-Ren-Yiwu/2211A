package com.retailersv.ads;

import java.io.Serializable;

public class AlertRule implements Serializable {
    // 规则唯一标识
    private String ruleId;

    // 适用表名（如：enriched_trade_stats, order_stats 等）
    private String table;

    // 指定的 SKU ID（支持 all、具体商品）
    private String skuId;

    // 报警阈值
    private Double threshold;

    // 是否启用
    private boolean enabled;

    // 规则说明（可用于告警描述）
    private String description;

    // 附加字段，可扩展表达式、维度等
    private String expression;

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSkuId() {
        return skuId;
    }

    public void setSkuId(String skuId) {
        this.skuId = skuId;
    }

    public Double getThreshold() {
        return threshold;
    }

    public void setThreshold(Double threshold) {
        this.threshold = threshold;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    @Override
    public String toString() {
        return "[Rule] " + ruleId + " (" + table + "): " + description + " => 阈值=" + threshold;
    }
}
