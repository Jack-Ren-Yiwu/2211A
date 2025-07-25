# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, lit

def insert_ads_city_stats(spark, dt):
    df = spark.table("tms.dws_trans_org_deliver_suc_nd") \
              .filter((col("dt") == dt) & (col("recent_days") == 7))
    df = df.groupBy("city_id", "city_name").agg(
        sum("order_count").alias("order_count")
    ).withColumn("order_amount", lit(0.00)) \
     .withColumn("trans_finish_count", lit(0)) \
     .withColumn("trans_finish_distance", lit(0.00)) \
     .withColumn("trans_finish_dur_sec", lit(0)) \
     .withColumn("avg_trans_finish_distance", lit(0.00)) \
     .withColumn("avg_trans_finish_dur_sec", lit(0)) \
     .withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(7))
    df.select("dt", "recent_days", "city_id", "city_name", "order_count", "order_amount",
              "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
              "avg_trans_finish_distance", "avg_trans_finish_dur_sec") \
      .write.mode("overwrite").insertInto("tms.ads_city_stats")

def insert_ads_express_city_stats(spark, dt):
    df = spark.table("tms.dws_trans_org_receive_nd") \
              .filter((col("dt") == dt) & (col("recent_days") == 7))
    df = df.groupBy("city_id", "city_name").agg(
        sum("order_count").alias("receive_order_count"),
        sum("order_amount").alias("receive_order_amount")
    ).withColumn("deliver_suc_count", lit(0)) \
     .withColumn("sort_count", lit(0)) \
     .withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(7))
    df.select("dt", "recent_days", "city_id", "city_name",
              "receive_order_count", "receive_order_amount",
              "deliver_suc_count", "sort_count") \
      .write.mode("append").insertInto("tms.ads_express_city_stats")

def insert_ads_express_org_stats(spark, dt):
    df = spark.table("tms.dws_trans_org_receive_nd") \
              .filter((col("dt") == dt) & (col("recent_days") == 7))
    df = df.groupBy("org_id", "org_name").agg(
        sum("order_count").alias("receive_order_count"),
        sum("order_amount").alias("receive_order_amount")
    ).withColumn("deliver_suc_count", lit(0)) \
     .withColumn("sort_count", lit(0)) \
     .withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(7))
    df.select("dt", "recent_days", "org_id", "org_name",
              "receive_order_count", "receive_order_amount",
              "deliver_suc_count", "sort_count") \
      .write.mode("append").insertInto("tms.ads_express_org_stats")

def insert_ads_express_stats(spark, dt):
    df = spark.table("tms.dws_trans_org_sort_nd") \
              .filter((col("dt") == dt) & (col("recent_days") == 7))
    df = df.groupBy().agg(
        sum("sort_count").alias("sort_count")
    ).withColumn("deliver_suc_count", lit(0)) \
     .withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(7))
    df.select("dt", "recent_days", "deliver_suc_count", "sort_count") \
      .write.mode("append").insertInto("tms.ads_express_stats")

def insert_ads_order_stats(spark, dt):
    df = spark.table("tms.dws_trade_org_cargo_type_order_nd") \
              .filter((col("dt") == dt) & (col("recent_days") == 7))
    df = df.groupBy().agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ).withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(7))
    df.select("dt", "recent_days", "order_count", "order_amount") \
      .write.mode("append").insertInto("tms.ads_order_stats")

def insert_ads_org_stats(spark, dt):
    df = spark.table("tms.dws_trans_org_receive_nd") \
              .filter((col("dt") == dt) & (col("recent_days") == 7))
    df = df.groupBy("org_id", "org_name").agg(
        sum("order_count").alias("order_count"),
        sum("order_amount").alias("order_amount")
    ).withColumn("trans_finish_count", lit(0)) \
     .withColumn("trans_finish_distance", lit(0.00)) \
     .withColumn("trans_finish_dur_sec", lit(0)) \
     .withColumn("avg_trans_finish_distance", lit(0.00)) \
     .withColumn("avg_trans_finish_dur_sec", lit(0)) \
     .withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(7))
    df.select("dt", "recent_days", "org_id", "org_name", "order_count", "order_amount",
              "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
              "avg_trans_finish_distance", "avg_trans_finish_dur_sec") \
      .write.mode("append").insertInto("tms.ads_org_stats")

def insert_ads_trans_order_stats(spark, dt):
    df = spark.table("tms.dws_trans_dispatch_nd") \
              .filter((col("dt") == dt) & (col("recent_days") == 7))
    df = df.groupBy().agg(
        sum("order_count").alias("dispatch_order_count"),
        sum("order_amount").alias("dispatch_order_amount")
    ).withColumn("receive_order_count", lit(0)) \
     .withColumn("receive_order_amount", lit(0.00)) \
     .withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(7))
    df.select("dt", "recent_days", "receive_order_count", "receive_order_amount",
              "dispatch_order_count", "dispatch_order_amount") \
      .write.mode("append").insertInto("tms.ads_trans_order_stats")

def insert_ads_trans_order_stats_td(spark, dt):
    df = spark.table("tms.dws_trans_bound_finish_td") \
              .filter(col("dt") == dt)
    df = df.groupBy().agg(
        sum("order_count").alias("bounding_order_count"),
        sum("order_amount").alias("bounding_order_amount")
    ).withColumn("dt", lit(dt))
    df.select("dt", "bounding_order_count", "bounding_order_amount") \
      .write.mode("append").insertInto("tms.ads_trans_order_stats_td")

def insert_ads_trans_stats(spark, dt):
    df = spark.table("tms.dws_trans_org_truck_model_type_trans_finish_1d") \
              .filter(col("dt") == dt)
    df = df.groupBy().agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ).withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(1))
    df.select("dt", "recent_days", "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec") \
      .write.mode("append").insertInto("tms.ads_trans_stats")

def insert_ads_truck_stats(spark, dt):
    df = spark.table("tms.dws_trans_shift_trans_finish_nd") \
              .filter((col("dt") == dt) & (col("recent_days") == 7))
    df = df.groupBy("truck_model_type", "truck_model_type_name").agg(
        sum("trans_finish_count").alias("trans_finish_count"),
        sum("trans_finish_distance").alias("trans_finish_distance"),
        sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    ).withColumn("avg_trans_finish_distance", lit(0.00)) \
     .withColumn("avg_trans_finish_dur_sec", lit(0)) \
     .withColumn("dt", lit(dt)) \
     .withColumn("recent_days", lit(7))
    df.select("dt", "recent_days", "truck_model_type", "truck_model_type_name",
              "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
              "avg_trans_finish_distance", "avg_trans_finish_dur_sec") \
      .write.mode("append").insertInto("tms.ads_truck_stats")

def main():
    spark = SparkSession.builder \
        .appName("ads_insert_all_full") \
        .enableHiveSupport() \
        .getOrCreate()

    # 待处理日期列表，改成你需要的日期
    dates = ["2025-07-09", "2025-07-10", "2025-07-11"]

    for dt in dates:
        print(f"开始处理日期: {dt}")
        insert_ads_city_stats(spark, dt)
        insert_ads_express_city_stats(spark, dt)
        insert_ads_express_org_stats(spark, dt)
        insert_ads_express_stats(spark, dt)
        insert_ads_order_stats(spark, dt)
        insert_ads_org_stats(spark, dt)
        insert_ads_trans_order_stats(spark, dt)
        insert_ads_trans_order_stats_td(spark, dt)
        insert_ads_trans_stats(spark, dt)
        insert_ads_truck_stats(spark, dt)

    spark.stop()

if __name__ == "__main__":
    main()
