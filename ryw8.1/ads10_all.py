# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, row_number, sum as _sum, round, desc
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("GenerateADSTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE gongdan10")

# =========================
# ADS 1：模块点击热度排行
# =========================
df_click = spark.table("dws_module_click_stats_1d")

# 按照点击次数降序排序，取前 N（比如前 20）
windowSpec = Window.partitionBy("dt").orderBy(desc("click_count"))

df_rank = df_click.withColumn("rank", row_number().over(windowSpec)) \
    .filter(col("rank") <= 20)

# 建表
spark.sql("DROP TABLE IF EXISTS ads_page_module_click_rank")
spark.sql("""
    CREATE EXTERNAL TABLE ads_page_module_click_rank (
        dt STRING COMMENT '统计日期',
        page_id STRING COMMENT '页面ID',
        module_id STRING COMMENT '模块ID',
        click_count INT COMMENT '点击次数',
        expo_count INT COMMENT '曝光次数',
        click_rate DOUBLE COMMENT '点击率',
        rank INT COMMENT '点击热度排行'
    )
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan10/ads/ads_page_module_click_rank'
""")

df_rank.select("dt", "page_id", "module_id", "click_count", "expo_count", "click_rate", "rank") \
    .write.mode("overwrite").insertInto("ads_page_module_click_rank")

# =========================
# ADS 2：页面引导转化趋势（近30天）
# =========================
df_conv = spark.table("dws_page_guide_conversion_1d")

df_trend = df_conv.groupBy("dt").agg(
    _sum("guide_jump_count").alias("total_jump"),
    _sum("click_count").alias("total_click"),
    _sum("expo_count").alias("total_expo"),
    round(_sum("guide_jump_count") / _sum("click_count"), 4).alias("click_conversion_rate"),
    round(_sum("guide_jump_count") / _sum("expo_count"), 4).alias("expo_conversion_rate")
)

# 建表
spark.sql("DROP TABLE IF EXISTS ads_page_conversion_trend")
spark.sql("""
    CREATE EXTERNAL TABLE ads_page_conversion_trend (
        dt STRING COMMENT '统计日期',
        total_jump BIGINT COMMENT '跳转次数',
        total_click BIGINT COMMENT '点击总数',
        total_expo BIGINT COMMENT '曝光总数',
        click_conversion_rate DOUBLE COMMENT '点击转化率',
        expo_conversion_rate DOUBLE COMMENT '曝光转化率'
    )
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan10/ads/ads_page_conversion_trend'
""")

df_trend.write.mode("overwrite").insertInto("ads_page_conversion_trend")

# =========================
# ADS 3：模块引导效果分析（曝光-点击-跳转）
# =========================
df_conv = spark.table("dws_page_guide_conversion_1d")

df_effect = df_conv.groupBy("module_id").agg(
    _sum("expo_count").alias("expo_count"),
    _sum("click_count").alias("click_count"),
    _sum("guide_jump_count").alias("jump_count"),
    round(_sum("click_count") / _sum("expo_count"), 4).alias("click_rate"),
    round(_sum("guide_jump_count") / _sum("click_count"), 4).alias("jump_click_rate")
)

# 建表
spark.sql("DROP TABLE IF EXISTS ads_module_guidance_effect")
spark.sql("""
    CREATE EXTERNAL TABLE ads_module_guidance_effect (
        module_id STRING COMMENT '模块ID',
        expo_count BIGINT COMMENT '曝光数',
        click_count BIGINT COMMENT '点击数',
        jump_count BIGINT COMMENT '跳转数',
        click_rate DOUBLE COMMENT '点击率',
        jump_click_rate DOUBLE COMMENT '点击转化率'
    )
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan10/ads/ads_module_guidance_effect'
""")

df_effect.write.mode("overwrite").insertInto("ads_module_guidance_effect")

# =========================
# ADS 4：异常模块识别（点击率低、跳转率低）
# =========================
# 条件：点击率低于 0.05 且跳转点击率低于 0.05
df_abnormal = df_effect.filter(
    ((col("click_rate") < 0.2) | (col("jump_click_rate") < 0.2)) &
    (col("expo_count") > 50)
)

# 建表
spark.sql("DROP TABLE IF EXISTS ads_page_abnormal_modules")
spark.sql("""
    CREATE EXTERNAL TABLE ads_page_abnormal_modules (
        module_id STRING COMMENT '模块ID',
        expo_count BIGINT COMMENT '曝光数',
        click_count BIGINT COMMENT '点击数',
        jump_count BIGINT COMMENT '跳转数',
        click_rate DOUBLE COMMENT '点击率',
        jump_click_rate DOUBLE COMMENT '点击转化率'
    )
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan10/ads/ads_page_abnormal_modules'
""")

df_abnormal.write.mode("overwrite").insertInto("ads_page_abnormal_modules")
