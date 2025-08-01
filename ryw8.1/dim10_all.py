# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, lit, udf, dayofweek
from pyspark.sql.types import StringType
import random

spark = SparkSession.builder \
    .appName("GenerateDimTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE gongdan10")

# ----------------------------
# 1. dim_product_full
# ----------------------------
ods_product_df = spark.table("ods_product_info")

# 添加品牌 & 类目层级（模拟）
brand_udf = udf(lambda x: "Brand_{}".format(random.randint(1, 20)), StringType())
cat1_udf = udf(lambda x: "CategoryLevel1_{}".format(random.choice(["A", "B", "C"])), StringType())
cat2_udf = udf(lambda x: "CategoryLevel2_{}".format(random.choice(["X", "Y", "Z"])), StringType())

dim_product = ods_product_df \
    .withColumn("brand_name", brand_udf(col("product_id"))) \
    .withColumn("level1_category", cat1_udf(col("category"))) \
    .withColumn("level2_category", cat2_udf(col("category")))

dim_product.write.mode("overwrite").saveAsTable("dim_product_full")

# ----------------------------
# 2. dim_page_module
# ----------------------------
ods_module_df = spark.table("ods_page_module_config")

# 添加 display_style 模拟字段（卡片、横幅、滑块、列表等）
style_udf = udf(lambda: random.choice(["Card", "Banner", "Slider", "List"]), StringType())
dim_module = ods_module_df.withColumn("display_style", style_udf())

dim_module.write.mode("overwrite").saveAsTable("dim_page_module")

# ----------------------------
# 3. dim_date
# ----------------------------
ods_visit_df = spark.table("ods_page_visit_log") \
    .withColumn("visit_date", substring("visit_time", 1, 10))

distinct_dates = ods_visit_df.select("visit_date").distinct() \
    .withColumn("date_key", col("visit_date")) \
    .withColumn("year", substring("visit_date", 1, 4)) \
    .withColumn("month", substring("visit_date", 6, 2)) \
    .withColumn("day", substring("visit_date", 9, 2)) \
    .withColumn("week_day", dayofweek(col("visit_date"))) \
    .withColumn("is_weekend", (dayofweek(col("visit_date")) >= 6).cast("string")) \
    .withColumn("is_holiday", lit("no"))  # 简化默认非节假日

distinct_dates.write.mode("overwrite").saveAsTable("dim_date")

# ----------------------------
# 4. dim_user
# ----------------------------
ods_user_df = spark.table("ods_user_info")

# 模拟性别与年龄段
gender_udf = udf(lambda: random.choice(["male", "female"]), StringType())
age_udf = udf(lambda: random.choice(["18-25", "26-35", "36-45", "46+"]), StringType())

dim_user = ods_user_df \
    .withColumn("gender", gender_udf()) \
    .withColumn("age_range", age_udf())

dim_user.write.mode("overwrite").saveAsTable("dim_user")
