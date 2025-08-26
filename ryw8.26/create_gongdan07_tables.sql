
-- 使用数据库
CREATE DATABASE IF NOT EXISTS gongdan07;
USE gongdan07;

-- 1. 用户基础信息表
CREATE TABLE IF NOT EXISTS user_info (
    user_id BIGINT PRIMARY KEY,
    gender VARCHAR(10),
    birthday DATE,
    age INT,
    province VARCHAR(50),
    city VARCHAR(50),
    register_time DATETIME,
    channel VARCHAR(50),
    level VARCHAR(20),
    score INT,
    is_blacklist BOOLEAN
);

-- 2. 用户行为日志表
CREATE TABLE IF NOT EXISTS user_behavior (
    user_id BIGINT,
    behavior_type VARCHAR(20),
    behavior_time DATETIME,
    sku_id VARCHAR(50),
    category1_id VARCHAR(50),
    category2_id VARCHAR(50),
    category3_id VARCHAR(50),
    page_id VARCHAR(50),
    platform VARCHAR(50),
    session_id VARCHAR(100)
);

-- 3. 订单信息表
CREATE TABLE IF NOT EXISTS order_info (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id BIGINT,
    sku_id VARCHAR(50),
    category3_id VARCHAR(50),
    order_time DATETIME,
    order_amount DOUBLE,
    pay_time DATETIME,
    order_status VARCHAR(20),
    coupon_amount DOUBLE,
    payment_channel VARCHAR(20)
);

-- 4. 退款信息表
CREATE TABLE IF NOT EXISTS refund_info (
    refund_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    user_id BIGINT,
    sku_id VARCHAR(50),
    refund_amount DOUBLE,
    refund_reason VARCHAR(100),
    refund_type VARCHAR(20),
    refund_time DATETIME,
    is_full_refund BOOLEAN
);

-- 5. 设备信息表
CREATE TABLE IF NOT EXISTS device_info (
    device_id VARCHAR(50) PRIMARY KEY,
    user_id BIGINT,
    device_type VARCHAR(20),
    os VARCHAR(20),
    brand VARCHAR(20),
    app_version VARCHAR(20),
    is_root BOOLEAN,
    first_use_time DATETIME,
    last_active_time DATETIME
);

-- 6. 商品信息表
CREATE TABLE IF NOT EXISTS sku_info (
    sku_id VARCHAR(50) PRIMARY KEY,
    spu_id VARCHAR(50),
    product_name VARCHAR(100),
    category3_id VARCHAR(50),
    price DOUBLE,
    brand_id VARCHAR(50),
    on_sale_date DATETIME,
    status VARCHAR(20)
);

-- 7. 类目信息表
CREATE TABLE IF NOT EXISTS category (
    category3_id VARCHAR(50) PRIMARY KEY,
    category3_name VARCHAR(50),
    category2_id VARCHAR(50),
    category1_id VARCHAR(50)
);

-- 8. 用户等级表
CREATE TABLE IF NOT EXISTS user_level (
    level VARCHAR(20) PRIMARY KEY,
    level_name VARCHAR(50),
    score_range VARCHAR(50),
    rule_desc VARCHAR(200)
);

-- 9. 省份地区表
CREATE TABLE IF NOT EXISTS province (
    province VARCHAR(50),
    city VARCHAR(50),
    region VARCHAR(50),
    avg_temp VARCHAR(10),
    area_level VARCHAR(20)
);

-- 10. 页面类型表
CREATE TABLE IF NOT EXISTS platform_page_type (
    page_id VARCHAR(50) PRIMARY KEY,
    page_name VARCHAR(50),
    page_type VARCHAR(20),
    usage_scenario VARCHAR(100)
);
