# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, countDistinct, when, lit
from datetime import datetime

spark = SparkSession.builder \
    .appName("GenerateDwsTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

dt = datetime.now().strftime("%Y-%m-%d")
spark.sql("USE gongdan09")

# 读取DWD层数据
dwd_page_view = spark.table("gongdan09.dwd_page_view_log").filter(col("dt") == dt)
dwd_page_path = spark.table("gongdan09.dwd_page_path_log").filter(col("dt") == dt)
dwd_order = spark.table("gongdan09.dwd_order_info").filter(col("dt") == dt)

# 1. dws_user_visit_summary_1d
dws_user_visit_summary_1d = dwd_page_view.groupBy("user_id") \
    .agg(
        count("*").alias("visit_cnt"),  # 总访问事件数
        _sum("duration").alias("total_duration"),  # 总停留时长
        countDistinct("page_id").alias("page_count")  # 访问页面数
    ) \
    .withColumn("bounce_flag", when((col("page_count") == 1) & (col("total_duration") < 5), lit(1)).otherwise(lit(0))) \
    .withColumn("dt", lit(dt))

dws_user_visit_summary_1d.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.dws_user_visit_summary_1d")

# 2. dws_page_visit_summary_1d
dws_page_visit_summary_1d = dwd_page_view.groupBy("page_id") \
    .agg(
        count("*").alias("pv"),
        countDistinct("user_id").alias("uv"),
        avg("duration").alias("avg_duration")
    ) \
    .withColumn("dt", lit(dt))

dws_page_visit_summary_1d.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.dws_page_visit_summary_1d")

# 3. dws_page_path_summary_1d
dws_page_path_summary_1d = dwd_page_path.groupBy("from_page", "to_page") \
    .agg(
        count("*").alias("cnt"),
        countDistinct("user_id").alias("uv")
    ) \
    .withColumn("dt", lit(dt))

dws_page_path_summary_1d.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.dws_page_path_summary_1d")

# 4. dws_user_order_summary_1d
# 支付成功状态
pay_success_status = ["paid", "shipped", "completed"]

dws_user_order_summary_1d = dwd_order.groupBy("user_id") \
    .agg(
        count("*").alias("order_cnt"),
        _sum("order_amount").alias("order_amt"),
        _sum(when(col("order_status").isin(pay_success_status), 1).otherwise(0)).alias("pay_cnt")
    ) \
    .withColumn("dt", lit(dt))

dws_user_order_summary_1d.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.dws_user_order_summary_1d")

print("✅ DWS 主题广表写入完成，分区：{}".format(dt))

spark.stop()
