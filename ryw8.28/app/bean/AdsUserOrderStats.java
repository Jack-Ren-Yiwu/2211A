package com.app.bean;


import lombok.Data;

@Data
public class AdsUserOrderStats {
    private String userId;
    private String windowStart;
    private String windowEnd;
    private Long orderCt;
    private Long refundCt;
    private Double orderAmountSum;
    private Double refundAmountSum;
    private Double couponAmountSum;

}