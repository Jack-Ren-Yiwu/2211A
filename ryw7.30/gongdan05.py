# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import lit
from datetime import datetime, timedelta
import random

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("HiveDwdETL_Enhanced") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

now = datetime.now()
dt = now.strftime("%Y-%m-%d")

# ----------------------
# 1. ODS层建表 & 扩充模拟数据（带连带行为）
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS gongdan.ods_user_action (
    user_id STRING,
    sku_id STRING,
    action_type STRING,
    action_time TIMESTAMP,
    device_type STRING,
    ip_address STRING,
    channel STRING,
    session_id STRING,
    refer_url STRING,
    user_agent STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/gongdan/ods_user_action'
""")

actions = ["exposure", "click", "cart", "fav", "pay", "detail"]
skus = ["sku_{0}".format(i) for i in range(1, 101)]  # 100个商品
devices = ["mobile", "pc", "tablet"]
channels = ["organic", "ads", "referral", "direct"]
user_agents = ["Mozilla/5.0", "Chrome/90.0", "Safari/537.36", "Edge/18.0"]
referers = ["google.com", "bing.com", "facebook.com", "twitter.com", "direct"]
main_skus = ["sku_1", "sku_2", "sku_3", "sku_4", "sku_5", "sku_6", "sku_7", "sku_8", "sku_9", "sku_10"]

def generate_ods_data():
    data = []
    for i in range(6000):
        user_id = "user_{0}".format(random.randint(1, 1000))
        # 60%概率访问主商品
        if random.random() < 0.6:
            sku_id = random.choice(main_skus)
        else:
            sku_id = random.choice(skus)
        action_type = random.choice(actions)
        action_time = now - timedelta(minutes=random.randint(0, 1440))
        device_type = random.choice(devices)
        ip_address = "192.168.{0}.{1}".format(random.randint(0, 255), random.randint(0, 255))
        channel = random.choice(channels)
        session_id = "sess_{0}".format(random.randint(1, 2000))
        refer_url = random.choice(referers)
        user_agent = random.choice(user_agents)

        data.append((user_id, sku_id, action_type, action_time, device_type, ip_address, channel, session_id, refer_url, user_agent))

        # 连带行为模拟：主商品访问后，40%概率添加关联商品行为，时间±20分钟
        if sku_id in main_skus and random.random() < 0.4:
            related_sku = random.choice([s for s in skus if s != sku_id])
            related_action_time = action_time + timedelta(minutes=random.randint(-20, 20))
            related_action_type = random.choice(actions)
            data.append((user_id, related_sku, related_action_type, related_action_time, device_type, ip_address, channel, session_id, refer_url, user_agent))
    return data

ods_schema = StructType([
    StructField("user_id", StringType()),
    StructField("sku_id", StringType()),
    StructField("action_type", StringType()),
    StructField("action_time", TimestampType()),
    StructField("device_type", StringType()),
    StructField("ip_address", StringType()),
    StructField("channel", StringType()),
    StructField("session_id", StringType()),
    StructField("refer_url", StringType()),
    StructField("user_agent", StringType())
])

ods_df = spark.createDataFrame(generate_ods_data(), schema=ods_schema).withColumn("dt", lit(dt))

spark.sql("set hive.exec.dynamic.partition=true")
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

ods_df.write.mode("overwrite").partitionBy("dt").format("parquet").saveAsTable("gongdan.ods_user_action")

# ----------------------
# 2. DIM层建表 & 模拟商品维度数据
spark.sql("""
CREATE TABLE IF NOT EXISTS gongdan.dim_sku (
    sku_id STRING,
    sku_name STRING,
    brand_id STRING,
    category_id STRING,
    price DOUBLE,
    listing_date TIMESTAMP,
    stock_quantity INT,
    supplier STRING,
    rating DOUBLE
)
STORED AS PARQUET
""")

brands = ["brand_{0}".format(i) for i in range(1, 21)]
categories = ["category_{0}".format(i) for i in range(1, 11)]
suppliers = ["supplier_{0}".format(i) for i in range(1, 6)]

sku_dim = []
for i in range(1, 101):
    sku_dim.append((
        "sku_{0}".format(i),
        "Product_{0}".format(i),
        random.choice(brands),
        random.choice(categories),
        round(random.uniform(10, 1000), 2),
        now - timedelta(days=random.randint(10, 730)),
        random.randint(0, 1000),
        random.choice(suppliers),
        round(random.uniform(1, 5), 2)
    ))

dim_sku_schema = StructType([
    StructField("sku_id", StringType()),
    StructField("sku_name", StringType()),
    StructField("brand_id", StringType()),
    StructField("category_id", StringType()),
    StructField("price", DoubleType()),
    StructField("listing_date", TimestampType()),
    StructField("stock_quantity", IntegerType()),
    StructField("supplier", StringType()),
    StructField("rating", DoubleType())
])

dim_sku_df = spark.createDataFrame(sku_dim, schema=dim_sku_schema)
dim_sku_df.write.mode("overwrite").format("parquet").saveAsTable("gongdan.dim_sku")

# ----------------------
# 3. DWD层宽表，关联ODS+DIM，建表写入
spark.sql("USE gongdan")

spark.sql("""
CREATE TABLE IF NOT EXISTS dwd_user_sku_action (
    user_id STRING,
    sku_id STRING,
    sku_name STRING,
    brand_id STRING,
    category_id STRING,
    price DOUBLE,
    listing_date TIMESTAMP,
    stock_quantity INT,
    supplier STRING,
    rating DOUBLE,
    action_type STRING,
    action_time TIMESTAMP,
    device_type STRING,
    ip_address STRING,
    channel STRING,
    session_id STRING,
    refer_url STRING,
    user_agent STRING
)
STORED AS PARQUET
""")

spark.sql("""
INSERT OVERWRITE TABLE dwd_user_sku_action
SELECT
    u.user_id,
    u.sku_id,
    s.sku_name,
    s.brand_id,
    s.category_id,
    s.price,
    s.listing_date,
    s.stock_quantity,
    s.supplier,
    s.rating,
    u.action_type,
    u.action_time,
    u.device_type,
    u.ip_address,
    u.channel,
    u.session_id,
    u.refer_url,
    u.user_agent
FROM gongdan.ods_user_action u
LEFT JOIN gongdan.dim_sku s ON u.sku_id = s.sku_id
WHERE u.dt = '{}'
""".format(dt))

# ----------------------
# 4. DWS层主商品行为连带，扩大时间窗口至30分钟，增加主商品数量
main_sku_df = spark.createDataFrame([(s,) for s in main_skus], ["main_sku"])
main_sku_df.createOrReplaceTempView("dim_main_sku")

spark.sql("""
CREATE TABLE IF NOT EXISTS dws_main_sku_related_actions (
    main_sku STRING,
    related_sku STRING,
    action_type STRING,
    action_cnt BIGINT
)
STORED AS PARQUET
""")

spark.sql("""
INSERT OVERWRITE TABLE dws_main_sku_related_actions
SELECT
    a.main_sku,
    b.sku_id AS related_sku,
    b.action_type,
    COUNT(*) AS action_cnt
FROM (
    SELECT u.user_id, u.sku_id AS main_sku, u.action_time
    FROM dwd_user_sku_action u
    JOIN (SELECT main_sku FROM dim_main_sku) m ON u.sku_id = m.main_sku
    WHERE u.action_time >= date_sub(current_date(), 7)
) a
JOIN dwd_user_sku_action b
ON a.user_id = b.user_id
AND b.action_time BETWEEN a.action_time - interval 30 minute AND a.action_time + interval 30 minute
AND a.main_sku != b.sku_id
GROUP BY a.main_sku, b.sku_id, b.action_type
""")

# ----------------------
# 5. ADS层主商品连带行为分析
spark.sql("""
CREATE TABLE IF NOT EXISTS ads_main_sku_related_behavior (
    main_sku STRING,
    related_sku STRING,
    related_click_cnt BIGINT,
    related_cart_cnt BIGINT,
    related_pay_cnt BIGINT
)
STORED AS PARQUET
""")

spark.sql("""
INSERT OVERWRITE TABLE ads_main_sku_related_behavior
SELECT
    main_sku,
    related_sku,
    SUM(CASE WHEN action_type = 'click' THEN action_cnt ELSE 0 END) AS related_click_cnt,
    SUM(CASE WHEN action_type = 'cart' THEN action_cnt ELSE 0 END) AS related_cart_cnt,
    SUM(CASE WHEN action_type = 'pay' THEN action_cnt ELSE 0 END) AS related_pay_cnt
FROM dws_main_sku_related_actions
GROUP BY main_sku, related_sku
""")

# ----------------------
# 6. ADS层详情页引导能力分析
spark.sql("""
CREATE TABLE IF NOT EXISTS ads_detail_page_guide_analysis (
    sku_id STRING,
    detail_uv BIGINT,
    click_uv BIGINT,
    cart_uv BIGINT,
    pay_uv BIGINT
)
STORED AS PARQUET
""")

spark.sql("""
INSERT OVERWRITE TABLE ads_detail_page_guide_analysis
SELECT
    sku_id,
    COUNT(DISTINCT CASE WHEN action_type = 'detail' THEN user_id END) AS detail_uv,
    COUNT(DISTINCT CASE WHEN action_type = 'click' THEN user_id END) AS click_uv,
    COUNT(DISTINCT CASE WHEN action_type = 'cart' THEN user_id END) AS cart_uv,
    COUNT(DISTINCT CASE WHEN action_type = 'pay' THEN user_id END) AS pay_uv
FROM dwd_user_sku_action
GROUP BY sku_id
""")

print("=== 主商品连带行为分析（ADS） ===")
spark.sql("SELECT * FROM ads_main_sku_related_behavior ORDER BY related_click_cnt DESC LIMIT 20").show(truncate=False)

print("=== 详情页引导能力分析（ADS） ===")
spark.sql("SELECT * FROM ads_detail_page_guide_analysis ORDER BY detail_uv DESC LIMIT 20").show(truncate=False)

spark.stop()
