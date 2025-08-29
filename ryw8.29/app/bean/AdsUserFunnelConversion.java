package com.app.bean;


import lombok.Data;

@Data
public class AdsUserFunnelConversion {
    private String userId;
    private Long viewCnt;
    private Long cartCnt;
    private Long payCnt;
    private Double viewToCartRate;
    private Double cartToPayRate;
    private Double viewToPayRate;


}