package com.app.dwd;

import lombok.Data;

@Data
public class OrderWide{
    // 公共字段
    private String order_id;
    private String user_id;
    private String sku_id;

    // order_info 字段
    private String category3_id;
    private String order_time;
    private Double order_amount;
    private String pay_time;
    private String order_status;
    private Double coupon_amount;
    private String payment_channel;

    // refund_info 字段
    private String refund_id;
    private Double refund_amount;
    private String refund_reason;
    private String refund_type;
    private String refund_time;
    private Boolean is_full_refund;

    public OrderWide(OrderInfo order, RefundInfo refund) {
        this.order_id = order.getOrder_id();
        this.user_id = order.getUser_id();
        this.sku_id = order.getSku_id();
        this.category3_id = order.getCategory3_id();
        this.order_time = order.getOrder_time();
        this.order_amount = order.getOrder_amount();
        this.pay_time = order.getPay_time();
        this.order_status = order.getOrder_status();
        this.coupon_amount = order.getCoupon_amount();
        this.payment_channel = order.getPayment_channel();

        this.refund_id = refund.getRefund_id();
        this.refund_amount = refund.getRefund_amount();
        this.refund_reason = refund.getRefund_reason();
        this.refund_type = refund.getRefund_type();
        this.refund_time = refund.getRefund_time();
        this.is_full_refund = refund.getIs_full_refund();
    }
}
