DROP TABLE IF EXISTS dim_sku_full;
CREATE EXTERNAL TABLE dim_sku_full
(
    `id`                   STRING COMMENT 'SKU_ID',
    `price`                DECIMAL(16, 2) COMMENT '商品价格',
    `sku_name`             STRING COMMENT '商品名称',
    `sku_desc`             STRING COMMENT '商品描述',
    `weight`               DECIMAL(16, 2) COMMENT '重量',
    `is_sale`              BOOLEAN COMMENT '是否在售',
    `spu_id`               STRING COMMENT 'SPU编号',
    `spu_name`             STRING COMMENT 'SPU名称',
    `category3_id`         STRING COMMENT '三级品类ID',
    `category3_name`       STRING COMMENT '三级品类名称',
    `category2_id`         STRING COMMENT '二级品类id',
    `category2_name`       STRING COMMENT '二级品类名称',
    `category1_id`         STRING COMMENT '一级品类ID',
    `category1_name`       STRING COMMENT '一级品类名称',
    `tm_id`                  STRING COMMENT '品牌ID',
    `tm_name`               STRING COMMENT '品牌名称',
    `sku_attr_values`      ARRAY<STRUCT<attr_id :STRING,
        value_id :STRING,
        attr_name :STRING,
        value_name:STRING>> COMMENT '平台属性',
    `sku_sale_attr_values` ARRAY<STRUCT<sale_attr_id :STRING,
        sale_attr_value_id :STRING,
        sale_attr_name :STRING,
        sale_attr_value_name:STRING>> COMMENT '销售属性',
    `create_time`          STRING COMMENT '创建时间'
) COMMENT '商品维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_sku_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
INSERT OVERWRITE TABLE dim_sku_full PARTITION (dt = '20250627')
SELECT
  sku.id,
  sku.price,
  sku.sku_name,
  sku.sku_desc,
  sku.weight,
  sku.is_sale,
  sku.spu_id,
  spu.spu_name,
  sku.category3_id,
  c3.name,
  c2.id,
  c2.name,
  c1.id,
  c1.name,
  sku.tm_id,
  tm.tm_name,
  attr.attrs,
  sale_attr.sale_attrs,
  sku.create_time
FROM ods_sku_info sku
LEFT JOIN ods_spu_info spu ON sku.spu_id = spu.id AND spu.ds = '20250627'
LEFT JOIN ods_base_category3 c3 ON sku.category3_id = c3.id AND c3.ds = '20250627'
LEFT JOIN ods_base_category2 c2 ON c3.category2_id = c2.id AND c2.ds = '20250627'
LEFT JOIN ods_base_category1 c1 ON c2.category1_id = c1.id AND c1.ds = '20250627'
LEFT JOIN ods_base_trademark tm ON sku.tm_id = tm.id AND tm.ds = '20250627'
LEFT JOIN (
  SELECT
    sku_id,
    collect_set(named_struct(
      'attr_id', cast(attr_id as STRING),
      'value_id', cast(value_id as STRING),
      'attr_name', attr_name,
      'value_name', value_name
    )) AS attrs
  FROM ods_sku_attr_value
  WHERE ds = '20250627'
  GROUP BY sku_id
) attr ON sku.id = attr.sku_id
LEFT JOIN (
  SELECT
    sku_id,
    collect_set(named_struct(
      'sale_attr_id', cast(sale_attr_id as STRING),
      'sale_attr_value_id', cast(sale_attr_value_id as STRING),
      'sale_attr_name', sale_attr_name,
      'sale_attr_value_name', sale_attr_value_name
    )) AS sale_attrs
  FROM ods_sku_sale_attr_value
  WHERE ds = '20250627'
  GROUP BY sku_id
) sale_attr ON sku.id = sale_attr.sku_id
WHERE sku.ds = '20250627';


DROP TABLE IF EXISTS dim_coupon_full;
CREATE EXTERNAL TABLE dim_coupon_full
(
    `id`                  STRING COMMENT '优惠券编号',
    `coupon_name`       STRING COMMENT '优惠券名称',
    `coupon_type_code` STRING COMMENT '优惠券类型编码',
    `coupon_type_name` STRING COMMENT '优惠券类型名称',
    `condition_amount` DECIMAL(16, 2) COMMENT '满额数',
    `condition_num`     BIGINT COMMENT '满件数',
    `activity_id`       STRING COMMENT '活动编号',
    `benefit_amount`   DECIMAL(16, 2) COMMENT '减免金额',
    `benefit_discount` DECIMAL(16, 2) COMMENT '折扣',
    `benefit_rule`     STRING COMMENT '优惠规则:满元*减*元，满*件打*折',
    `create_time`       STRING COMMENT '创建时间',
    `range_type_code`  STRING COMMENT '优惠范围类型编码',
    `range_type_name`  STRING COMMENT '优惠范围类型名称',
    `limit_num`         BIGINT COMMENT '最多领取次数',
    `taken_count`       BIGINT COMMENT '已领取次数',
    `start_time`        STRING COMMENT '可以领取的开始时间',
    `end_time`          STRING COMMENT '可以领取的结束时间',
    `operate_time`      STRING COMMENT '修改时间',
    `expire_time`       STRING COMMENT '过期时间'
) COMMENT '优惠券维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_coupon_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_coupon_full partition(dt='20250627')
SELECT
    ci.id,
    ci.coupon_name,
    CAST(ci.coupon_type AS STRING) AS coupon_type_code,
    coupon_dic.dic_name AS coupon_type_name,
    ci.condition_amount,
    ci.condition_num,
    ci.activity_id,
    ci.benefit_amount,
    ci.benefit_discount,
    CASE CAST(ci.coupon_type AS STRING)
        WHEN '3201' THEN CONCAT('满', ci.condition_amount, '元减', ci.benefit_amount, '元')
        WHEN '3202' THEN CONCAT('满', ci.condition_num, '件打', ci.benefit_discount, ' 折')
        WHEN '3203' THEN CONCAT('减', ci.benefit_amount, '元')
    END AS benefit_rule,
    ci.create_time,
    CAST(ci.range_type AS STRING) AS range_type_code,
    range_dic.dic_name AS range_type_name,
    ci.limit_num,
    ci.taken_count,
    ci.start_time,
    ci.end_time,
    ci.operate_time,
    ci.expire_time
FROM
(
    SELECT
        id,
        coupon_name,
        coupon_type,
        condition_amount,
        condition_num,
        activity_id,
        benefit_amount,
        benefit_discount,
        create_time,
        range_type,
        limit_num,
        taken_count,
        start_time,
        end_time,
        operate_time,
        expire_time
    FROM ods_coupon_info
    WHERE ds='20250627'
) ci
LEFT JOIN (
    SELECT
        dic_code,
        dic_name
    FROM ods_base_dic
    WHERE ds='20250627'
      AND parent_code='32'
) coupon_dic ON ci.coupon_type = coupon_dic.dic_code
LEFT JOIN (
    SELECT
       dic_code,
        dic_name
    FROM ods_base_dic
    WHERE ds='20250627'
      AND parent_code='33'
) range_dic ON ci.range_type = range_dic.dic_code;

DROP TABLE IF EXISTS dim_activity_full;
CREATE EXTERNAL TABLE dim_activity_full
(
    `activity_rule_id`   STRING COMMENT '活动规则ID',
    `activity_id`         STRING COMMENT '活动ID',
    `activity_name`       STRING COMMENT '活动名称',
    `activity_type_code` STRING COMMENT '活动类型编码',
    `activity_type_name` STRING COMMENT '活动类型名称',
    `activity_desc`       STRING COMMENT '活动描述',
    `start_time`           STRING COMMENT '开始时间',
    `end_time`             STRING COMMENT '结束时间',
    `create_time`          STRING COMMENT '创建时间',
    `condition_amount`    DECIMAL(16, 2) COMMENT '满减金额',
    `condition_num`       BIGINT COMMENT '满减件数',
    `benefit_amount`      DECIMAL(16, 2) COMMENT '优惠金额',
    `benefit_discount`   DECIMAL(16, 2) COMMENT '优惠折扣',
    `benefit_rule`        STRING COMMENT '优惠规则',
    `benefit_level`       STRING COMMENT '优惠级别'
) COMMENT '活动维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_activity_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_activity_full partition(dt='20250627')
select
    rule.id,
    info.id,
    activity_name,
    rule.activity_type,
    dic.dic_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    case rule.activity_type
        when '3101' then concat('满',condition_amount,'元减',benefit_amount,'元')
        when '3102' then concat('满',condition_num,'件打', benefit_discount,' 折')
        when '3103' then concat('打', benefit_discount,'折')
    end benefit_rule,
    benefit_level
from
(
    select
        id,
        activity_id,
        activity_type,
        condition_amount,
        condition_num,
        benefit_amount,
        benefit_discount,
        benefit_level
    from ods_activity_rule
    where ds='20250627'
)rule
left join
(
    select
        id,
        activity_name,
        activity_type,
        activity_desc,
        start_time,
        end_time,
        create_time
    from ods_activity_info
    where ds='20250627'
)info
on rule.activity_id=info.id
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic
    where ds='20250627'
    and parent_code='31'
)dic
on rule.activity_type=dic.dic_code;


DROP TABLE IF EXISTS dim_province_full;
CREATE EXTERNAL TABLE dim_province_full
(
    `id`              STRING COMMENT '省份ID',
    `province_name` STRING COMMENT '省份名称',
    `area_code`     STRING COMMENT '地区编码',
    `iso_code`      STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2`    STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `region_id`     STRING COMMENT '地区ID',
    `region_name`   STRING COMMENT '地区名称'
) COMMENT '地区维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_province_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_province_full partition(dt='20250627')
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
(
    select
        id,
        name,
        region_id,
        area_code,
        iso_code,
        iso_3166_2
    from ods_base_province
    where ds='20250627'
)province
left join
(
    select
        id,
        region_name
    from ods_base_region
    where ds='20250627'
)region
on province.region_id=region.id;


DROP TABLE IF EXISTS dim_promotion_pos_full;
CREATE EXTERNAL TABLE dim_promotion_pos_full
(
    `id`                 STRING COMMENT '营销坑位ID',
    `pos_location`     STRING COMMENT '营销坑位位置',
    `pos_type`          STRING COMMENT '营销坑位类型 ',
    `promotion_type`   STRING COMMENT '营销类型',
    `create_time`       STRING COMMENT '创建时间',
    `operate_time`      STRING COMMENT '修改时间'
) COMMENT '营销坑位维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_promotion_pos_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_promotion_pos_full partition(dt='20250627')
select
    `id`,
    `pos_location`,
    `pos_type`,
    `promotion_type`,
    `create_time`,
    `operate_time`
from ods_promotion_pos
where ds='20250627';

DROP TABLE IF EXISTS dim_promotion_refer_full;
CREATE EXTERNAL TABLE dim_promotion_refer_full
(
    `id`                    STRING COMMENT '营销渠道ID',
    `refer_name`          STRING COMMENT '营销渠道名称',
    `create_time`         STRING COMMENT '创建时间',
    `operate_time`        STRING COMMENT '修改时间'
) COMMENT '营销渠道维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_promotion_refer_full/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_promotion_refer_full partition(dt='20250627')
select
    `id`,
    `refer_name`,
    `create_time`,
    `operate_time`
from ods_promotion_refer
where ds='20250627';



DROP TABLE IF EXISTS dim_date;
CREATE EXTERNAL TABLE dim_date
(
    `date_id`    STRING COMMENT '日期ID',
    `week_id`    STRING COMMENT '周ID,一年中的第几周',
    `week_day`   STRING COMMENT '周几',
    `day`         STRING COMMENT '每月的第几天',
    `month`       STRING COMMENT '一年中的第几月',
    `quarter`    STRING COMMENT '一年中的第几季度',
    `year`        STRING COMMENT '年份',
    `is_workday` STRING COMMENT '是否是工作日',
    `holiday_id` STRING COMMENT '节假日'
) COMMENT '日期维度表'
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_date/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_date select * from tmp_dim_date_info;

DROP TABLE IF EXISTS dim_user_zip;
CREATE EXTERNAL TABLE dim_user_zip
(
    `id`           STRING COMMENT '用户ID',
    `name`         STRING COMMENT '用户姓名',
    `phone_num`    STRING COMMENT '手机号码',
    `email`        STRING COMMENT '邮箱',
    `user_level`   STRING COMMENT '用户等级',
    `birthday`     STRING COMMENT '生日',
    `gender`       STRING COMMENT '性别',
    `create_time`  STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `start_date`   STRING COMMENT '开始日期',
    `end_date`     STRING COMMENT '结束日期'
) COMMENT '用户维度表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/warehouse/gmall/dim/dim_user_zip/'
    TBLPROPERTIES ('orc.compress' = 'snappy');
insert overwrite table dim_user_zip partition (dt = '20250627')
select id,
       concat(substr(name, 1, 1), '*')       as         name,
       if(phone_num regexp '^(13[0-9]|14[01456879]|15[0-35-9]|16[2567]|17[0-8]|18[0-9]|19[0-35-9])\\d{8}$',
          concat(substr(phone_num, 1, 3), '*'), null) as phone_num,
       if(email regexp '^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$',
          concat('*@', split(email, '@')[1]), null) as  email,
       user_level,
       birthday,
       gender,
       create_time,
       operate_time,
       '20250627'                                        start_date,
       '9999-12-31'                                        end_date
from ods_user_info
where ds = '20250627';

-- 开启动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- 插入每日变更数据
INSERT OVERWRITE TABLE dim_user_zip PARTITION (dt)
SELECT
    id,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date,
    CASE
        WHEN rn = 1 THEN '9999-12-31'
        ELSE '20250626'
    END AS dt
FROM (
    SELECT
        id,
        -- 脱敏处理
        CASE WHEN name IS NOT NULL THEN CONCAT(SUBSTRING(name, 1, 1), '*') ELSE NULL END AS name,
        CASE
            WHEN phone_num RLIKE '^1[3-9][0-9]{9}$' THEN CONCAT(SUBSTRING(phone_num, 1, 3), '****', SUBSTRING(phone_num, 8))
            ELSE NULL
        END AS phone_num,
        CASE
            WHEN email RLIKE '^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$' THEN CONCAT('***@', SPLIT(email, '@')[1])
            ELSE NULL
        END AS email,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        '20250627' AS start_date,
        CASE
            WHEN rn = 1 THEN '9999-12-31'
            ELSE '20250626'
        END AS end_date,
        rn
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY id ORDER BY start_date DESC) AS rn
        FROM (
            -- 合并变更数据 + 历史快照
            SELECT
          CAST(id AS STRING) AS id,
  CAST(name AS STRING) AS name,
  CAST(phone_num AS STRING) AS phone_num,
  CAST(email AS STRING) AS email,
  CAST(user_level AS STRING) AS user_level,
  CAST(birthday AS STRING) AS birthday,  -- ⭐ 这里关键
  CAST(gender AS STRING) AS gender,
  CAST(create_time AS STRING) AS create_time,
  CAST(operate_time AS STRING) AS operate_time,
  '20250627' AS start_date,
  '9999-12-31' AS end_date
            FROM ods_user_info
            WHERE ds = '20250627'

            UNION ALL

            SELECT
                id,
                name,
                phone_num,
                email,
                user_level,
                birthday,
                gender,
                create_time,
                operate_time,
                start_date,
                end_date
            FROM dim_user_zip
            WHERE dt = '9999-12-31'
        ) merged_data
    ) deduped_data
) final_data;


