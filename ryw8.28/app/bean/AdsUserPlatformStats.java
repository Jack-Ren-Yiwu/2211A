package com.app.bean;



import lombok.Data;

@Data
public class AdsUserPlatformStats {
    private String platform;
    private Long userCnt;
    private Long orderCnt;
    private Double payAmount;
    private Double avgOrderAmount;


}