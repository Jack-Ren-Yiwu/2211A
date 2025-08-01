# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct, count, round, when, lit

spark = SparkSession.builder \
    .appName("GenerateDWSTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE gongdan10")

# =========================
# DWS 1：模块点击统计（点击/曝光/点击率）
# =========================

expo_df = spark.table("dwd_module_exposure_log") \
    .withColumn("dt", to_date("expose_time"))

click_df = spark.table("dwd_page_click_log") \
    .withColumn("dt", to_date("visit_time"))

# 曝光统计
expo_stats = expo_df.groupBy("dt", "page_id", "module_id").agg(
    count("*").alias("expo_count"),
    countDistinct("user_id").alias("expo_user_count")
)

# 点击统计
click_stats = click_df.groupBy("dt", "page_id", "module_id").agg(
    count("*").alias("click_count"),
    countDistinct("user_id").alias("click_user_count")
)

# 合并曝光+点击
module_stats_df = expo_stats.join(
    click_stats,
    on=["dt", "page_id", "module_id"],
    how="left"
).na.fill(0) \
 .withColumn("click_rate", round(col("click_count") / when(col("expo_count") == 0, lit(1)).otherwise(col("expo_count")), 4))

# 写入 Hive 表
spark.sql("DROP TABLE IF EXISTS dws_module_click_stats_1d")
spark.sql("""
    CREATE EXTERNAL TABLE dws_module_click_stats_1d (
        dt STRING COMMENT '统计日期',
        page_id STRING COMMENT '页面ID',
        module_id STRING COMMENT '模块ID',
        expo_count INT COMMENT '曝光次数',
        expo_user_count INT COMMENT '曝光用户数',
        click_count INT COMMENT '点击次数',
        click_user_count INT COMMENT '点击用户数',
        click_rate DOUBLE COMMENT '点击率'
    )
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan10/dws/dws_module_click_stats_1d'
""")
module_stats_df.write.mode("overwrite").insertInto("dws_module_click_stats_1d")

# =========================
# DWS 2：页面引导跳转转化（跳转商品数/点击/曝光）
# =========================

jump_df = spark.table("dwd_page_jump_to_product") \
    .withColumn("dt", to_date("jump_time"))

# 跳转统计
jump_stats_df = jump_df.groupBy("dt", "page_id", "module_id").agg(
    count("*").alias("guide_jump_count"),
    countDistinct("product_id").alias("guide_sku_count")
)

# 合并点击、曝光数据
guide_stats_df = jump_stats_df.join(
    module_stats_df.select("dt", "page_id", "module_id", "click_count", "expo_count"),
    on=["dt", "page_id", "module_id"],
    how="left"
).na.fill(0) \
 .withColumn("jump_click_rate", round(col("guide_jump_count") / when(col("click_count") == 0, lit(1)).otherwise(col("click_count")), 4)) \
 .withColumn("jump_expo_rate", round(col("guide_jump_count") / when(col("expo_count") == 0, lit(1)).otherwise(col("expo_count")), 4))

# 写入 Hive 表
spark.sql("DROP TABLE IF EXISTS dws_page_guide_conversion_1d")
spark.sql("""
    CREATE EXTERNAL TABLE dws_page_guide_conversion_1d (
        dt STRING COMMENT '统计日期',
        page_id STRING COMMENT '页面ID',
        module_id STRING COMMENT '模块ID',
        guide_jump_count INT COMMENT '跳转商品总数',
        guide_sku_count INT COMMENT '跳转商品SKU数',
        click_count INT COMMENT '点击数',
        expo_count INT COMMENT '曝光数',
        jump_click_rate DOUBLE COMMENT '点击转化率',
        jump_expo_rate DOUBLE COMMENT '曝光转化率'
    )
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan10/dws/dws_page_guide_conversion_1d'
""")
guide_stats_df.write.mode("overwrite").insertInto("dws_page_guide_conversion_1d")

# =========================
# DWS 3：页面引导的商品转化明细
# =========================

product_from_page_df = jump_df.groupBy("dt", "page_id", "module_id", "product_id").agg(
    count("*").alias("jump_count"),
    countDistinct("user_id").alias("user_count")
)

spark.sql("DROP TABLE IF EXISTS dws_product_from_page_1d")
spark.sql("""
    CREATE EXTERNAL TABLE dws_product_from_page_1d (
        dt STRING COMMENT '统计日期',
        page_id STRING COMMENT '页面ID',
        module_id STRING COMMENT '模块ID',
        product_id STRING COMMENT '商品ID',
        jump_count INT COMMENT '跳转次数',
        user_count INT COMMENT '跳转用户数'
    )
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan10/dws/dws_product_from_page_1d'
""")
product_from_page_df.write.mode("overwrite").insertInto("dws_product_from_page_1d")
