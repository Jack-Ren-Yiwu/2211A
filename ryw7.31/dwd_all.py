# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lead
from pyspark.sql.window import Window
from datetime import datetime

spark = SparkSession.builder \
    .appName("GenerateDimTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()


dt = datetime.now().strftime("%Y-%m-%d")
spark.sql("USE gongdan09")

# === 1. dwd_page_view_log ===
# 关联 ods_app_visit_log 与 dim_page_info，保留停留行为（event_type='stay'）

ods_visit = spark.table("gongdan09.ods_app_visit_log").filter(col("dt") == dt)
dim_page = spark.table("gongdan09.dim_page_info")

dwd_page_view = ods_visit.alias("o") \
    .join(dim_page.alias("p"), "page_id", "left") \
    .filter(col("o.event_type") == "stay") \
    .select(
        col("o.user_id"),
        col("o.session_id"),
        col("o.device_id"),
        col("o.page_id"),
        col("p.page_name"),
        col("p.page_type"),
        col("p.module_name"),
        col("o.ref_page_id").alias("refer_page_id"),
        col("o.duration"),
        col("o.event_time"),
        col("o.os_type"),
        col("o.app_version"),
        col("o.channel"),
        col("o.province"),
        col("o.city"),
        col("o.ip")
    ) \
    .withColumn("dt", lit(dt))

dwd_page_view.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.dwd_page_view_log")

# === 2. dwd_user_entry_log ===
# 用户首次入店行为（页面是首页/详情/活动页）

entry_pages = ["home", "detail", "campaign"]

dwd_user_entry = ods_visit \
    .filter(col("page_id").isin(entry_pages)) \
    .select(
        col("user_id"),
        col("session_id"),
        col("device_id"),
        col("page_id").alias("entry_page"),
        col("event_time"),
        col("province"),
        col("city")
    ) \
    .withColumn("dt", lit(dt))

dwd_user_entry.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.dwd_user_entry_log")

# === 3. dwd_page_path_log ===
# 构造页面跳转路径，利用窗口函数

window_spec = Window.partitionBy("user_id", "session_id").orderBy("event_time")

# 构建页面跳转路径
page_path = ods_visit \
    .select("user_id", "session_id", "page_id", "event_time") \
    .withColumn("next_page", lead("page_id").over(window_spec)) \
    .withColumn("next_event_time", lead("event_time").over(window_spec)) \
    .filter(col("next_page").isNotNull()) \
    .select(
        col("page_id").alias("from_page"),
        col("next_page").alias("to_page"),
        col("user_id"),
        col("session_id"),
        col("event_time"),
    ) \
    .withColumn("dt", lit(dt))

# 可选：打印一部分数据用于验证
page_path.show(10, truncate=False)

# 写入 DWD 页面跳转路径明细表
page_path.write.mode("overwrite") \
    .partitionBy("dt") \
    .format("parquet") \
    .saveAsTable("gongdan09.dwd_page_path_log")

print("✅ dwd_page_path_log 写入完成，当前分区：{}".format(dt))

# === 4. dwd_order_info ===
# 关联 ods_order_info 和 ods_product_info，筛选有效订单状态

ods_order = spark.table("gongdan09.ods_order_info")
ods_product = spark.table("gongdan09.ods_product_info")

dwd_order = ods_order.alias("o") \
    .join(ods_product.alias("p"), "sku_id", "left") \
    .filter(col("o.order_status").isin(["created", "paid", "shipped", "completed"])) \
    .select(
        col("o.order_id"),
        col("o.user_id"),
        col("o.sku_id"),
        col("p.product_name"),
        col("p.category"),
        col("p.brand"),
        col("o.order_amount"),
        col("o.order_status"),
        col("o.order_time")
    ) \
    .withColumn("dt", lit(dt))

dwd_order.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.dwd_order_info")

print("✅ 基于ODS和DIM的DWD层表写入完成")

spark.stop()
