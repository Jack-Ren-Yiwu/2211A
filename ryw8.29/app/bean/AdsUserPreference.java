package com.app.bean;

import lombok.Data;

@Data
public class AdsUserPreference {
    private String userId;
    private String topViewBrand;
    private String topCartBrand;
    private String topPayBrand;
    private String preferCategory3;
    private String preferPlatform;


}