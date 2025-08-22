package com.retailersv.alert;

import lombok.Data;



@Data
public class AlertRule {
    private Integer id;
    private String table_name;
    private String field_name;
    private String operator;
    private Double threshold;
    private String description;
    private Integer status;
    private String rule_type;

}
