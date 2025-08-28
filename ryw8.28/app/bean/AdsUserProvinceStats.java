package com.app.bean;



import lombok.Data;

@Data
public class AdsUserProvinceStats {
    private String province;
    private Long userCnt;
    private Double orderAmount;
    private Long payUserCnt;
    private Double refundAmount;


}