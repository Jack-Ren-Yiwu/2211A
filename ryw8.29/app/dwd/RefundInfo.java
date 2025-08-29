package com.app.dwd;

import lombok.Data;




@Data
public class RefundInfo {
    private String refund_id;
    private String order_id;
    private String user_id;
    private String sku_id;
    private Double refund_amount;
    private String refund_reason;
    private String refund_type;
    private String refund_time;
    private Boolean is_full_refund;

    public long getRefundTimeMillis() {
        try {
            return Long.parseLong(refund_time);
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }
}
