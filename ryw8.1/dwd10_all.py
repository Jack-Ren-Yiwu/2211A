# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import substring

spark = SparkSession.builder \
    .appName("GenerateDimTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()
spark.sql("USE gongdan10")

# ----------------------------
# dwd_page_click_log
# ----------------------------
visit_df = spark.table("ods_page_visit_log")
module_dim = spark.table("dim_page_module")

click_dwd = visit_df.alias("v") \
    .join(module_dim.alias("m"), col("v.module_id") == col("m.module_id"), "left") \
    .select(
        col("v.visit_id"),
        col("v.user_id"),
        col("v.visit_time"),
        col("v.page_id"),
        col("v.module_id"),
        col("m.module_name"),
        col("m.module_type"),
        col("m.display_style"),
        col("v.click_position"),
        col("v.ip"),
        col("v.device")
    )

click_dwd.write.mode("overwrite").saveAsTable("dwd_page_click_log")

# ----------------------------
# dwd_page_jump_to_product
# ----------------------------
jump_df = spark.table("ods_page_to_product_log")
product_dim = spark.table("dim_product_full")
module_dim = spark.table("dim_page_module")

jump_dwd = jump_df.alias("j") \
    .join(product_dim.alias("p"), col("j.product_id") == col("p.product_id"), "left") \
    .join(module_dim.alias("m"), col("j.module_id") == col("m.module_id"), "left") \
    .select(
        col("j.jump_id"),
        col("j.user_id"),
        col("j.page_id"),
        col("j.module_id"),
        col("m.module_name"),
        col("m.module_type"),
        col("p.product_id"),
        col("p.category"),
        col("p.brand_name"),
        col("p.level1_category"),
        col("p.level2_category"),
        col("p.price"),
        col("j.jump_time")
    )

jump_dwd.write.mode("overwrite").saveAsTable("dwd_page_jump_to_product")

# ----------------------------
# dwd_module_exposure_log
# ----------------------------
# 原则：每个页面访问一次模块点击行为，默认模块已曝光
exposure_df = visit_df.alias("v") \
    .join(module_dim.alias("m"), col("v.module_id") == col("m.module_id"), "left") \
    .select(
        col("v.visit_id"),
        col("v.user_id"),
        col("v.visit_time").alias("expose_time"),
        col("v.page_id"),
        col("v.module_id"),
        col("m.module_name"),
        col("m.module_type"),
        col("m.display_style"),
        lit("auto_click_before").alias("expose_reason")
    )

exposure_df.write.mode("overwrite").saveAsTable("dwd_module_exposure_log")

