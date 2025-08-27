package com.app.dwd;

import lombok.Data;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Data
public class OrderInfo {
    private String order_id;
    private String user_id;
    private String sku_id;
    private String category3_id;
    private String order_time;
    private Double order_amount;
    private String pay_time;
    private String order_status;
    private Double coupon_amount;
    private String payment_channel;

    public long getOrderTimeMillis() {
        try {
            return Long.parseLong(order_time);  // 直接解析毫秒时间戳
        } catch (Exception e) {
            return System.currentTimeMillis();  // fallback
        }
    }
}
