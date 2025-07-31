# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, avg, max as _max, sum as _sum, when, lit, concat_ws, collect_list
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from datetime import datetime

spark = SparkSession.builder \
    .appName("GenerateAdsTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

dt = datetime.now().strftime("%Y-%m-%d")
spark.sql("USE gongdan09")

# 读取 DWS 表
dws_page_visit = spark.table("gongdan09.dws_page_visit_summary_1d").filter(col("dt") == dt)
dws_page_path = spark.table("gongdan09.dws_page_path_summary_1d").filter(col("dt") == dt)
dws_user_visit = spark.table("gongdan09.dws_user_visit_summary_1d").filter(col("dt") == dt)
dws_user_order = spark.table("gongdan09.dws_user_order_summary_1d").filter(col("dt") == dt)

# 1. ads_page_visit_topn
ads_page_visit_topn = dws_page_visit.orderBy(col("uv").desc()) \
    .withColumn("dt", lit(dt))

ads_page_visit_topn.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.ads_page_visit_topn")

# 2. ads_page_stay_analysis
dwd_page_view = spark.table("gongdan09.dwd_page_view_log").filter(col("dt") == dt)

# max_stay
page_max_stay = dwd_page_view.groupBy("page_id").agg(_max("duration").alias("max_stay"))

# 计算跳出率
user_page_visit = dwd_page_view.groupBy("page_id", "user_id") \
    .agg(
        countDistinct("event_time").alias("visit_times"),
        _sum("duration").alias("total_duration")
    )

user_page_bounce = user_page_visit.withColumn("is_bounce",
    when((col("visit_times") == 1) & (col("total_duration") < 5), 1).otherwise(0))

bounce_stats = user_page_bounce.groupBy("page_id") \
    .agg(
        _sum("is_bounce").alias("bounce_user_cnt"),
        countDistinct("user_id").alias("user_cnt")
    ) \
    .withColumn("bounce_rate",
                when(col("user_cnt") > 0, col("bounce_user_cnt") / col("user_cnt")).otherwise(0))

ads_page_stay_analysis = dws_page_visit.join(page_max_stay, "page_id", "left") \
    .join(bounce_stats.select("page_id", "bounce_rate"), "page_id", "left") \
    .select(
        col("page_id"),
        col("avg_duration").alias("avg_stay"),
        col("max_stay"),
        col("bounce_rate")
    ).withColumn("dt", lit(dt))

ads_page_stay_analysis.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.ads_page_stay_analysis")

# 3. ads_page_path_analysis
from pyspark.sql.functions import max as _max

from_page_uv = dws_page_path.groupBy("from_page").agg(_max("uv").alias("from_page_uv"))

ads_page_path_analysis = dws_page_path.join(from_page_uv, "from_page", "left") \
    .withColumn("jump_rate", col("cnt") / col("from_page_uv")) \
    .select(
        col("from_page"),
        col("to_page"),
        col("cnt"),
        col("jump_rate")
    ).withColumn("dt", lit(dt))

ads_page_path_analysis.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.ads_page_path_analysis")

# 4. ads_entry_page_conversion
dwd_user_entry = spark.table("gongdan09.dwd_user_entry_log").filter(col("dt") == dt)
dwd_order = spark.table("gongdan09.dwd_order_info").filter(col("dt") == dt)

entry_order = dwd_user_entry.alias("e") \
    .join(dwd_order.alias("o"), "user_id", "inner") \
    .select(
        col("e.entry_page").alias("page_id"),
        col("o.order_id"),
        col("o.order_status")
    )

pay_success_status = ["paid", "shipped", "completed"]

ads_entry_page_conversion = entry_order.groupBy("page_id") \
    .agg(
        countDistinct("order_id").alias("order_cnt"),
        _sum(when(col("order_status").isin(pay_success_status), 1).otherwise(0)).alias("pay_cnt")
    ) \
    .withColumn("conversion_rate",
                when(col("order_cnt") > 0, col("pay_cnt") / col("order_cnt")).otherwise(0)) \
    .withColumn("dt", lit(dt))

ads_entry_page_conversion.write.mode("overwrite").partitionBy("dt").format("parquet") \
    .saveAsTable("gongdan09.ads_entry_page_conversion")

# 5. ads_user_flow_path
dwd_page_path_log = spark.table("gongdan09.dwd_page_path_log").filter(col("dt") == dt)
dwd_page_view_log = spark.table("gongdan09.dwd_page_view_log").filter(col("dt") == dt)

# 先给 dwd_page_path_log 排序并编号
window_spec = Window.partitionBy("user_id", "session_id").orderBy("event_time")
dwd_page_path_log_ordered = dwd_page_path_log.withColumn("rn", row_number().over(window_spec))

# 按 rn 排序，收集路径（from_page->to_page）
from pyspark.sql.functions import expr

user_path = dwd_page_path_log_ordered.groupBy("user_id", "session_id") \
    .agg(
        concat_ws("->", collect_list(expr("concat(from_page, '->', to_page)"))).alias("path_chain")
    )

# 计算用户每日总停留时长
user_total_duration = dwd_page_view_log.groupBy("user_id") \
    .agg(_sum("duration").alias("total_duration"))

# 关联日期，session和路径
user_flow_path = user_path.join(user_total_duration, "user_id", "left") \
    .withColumn("visit_date", lit(dt)) \
    .select("user_id", "visit_date", "path_chain", "total_duration")

ads_user_flow_path = user_flow_path

ads_user_flow_path.write.mode("overwrite").partitionBy("visit_date").format("parquet") \
    .saveAsTable("gongdan09.ads_user_flow_path")

print("✅ ADS 层指标分析表写入完成，分区：{}".format(dt))

spark.stop()
