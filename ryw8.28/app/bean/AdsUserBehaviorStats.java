package com.app.bean;



import lombok.Data;

@Data
public class AdsUserBehaviorStats {
    private String userId;
    private Long pv;
    private Long click;
    private Long cart;
    private Long pay;
    private Long sessionCt;
    private Long bounceCt;
    private Double avgStayDuration;

}