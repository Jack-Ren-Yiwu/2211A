# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, round as _round, sum as _sum
import datetime
import sys

def generate_date_string(offset=1):
    return (datetime.datetime.now() - datetime.timedelta(days=offset)).strftime("%Y%m%d")

def generate_ads_overview(dt):
    spark = SparkSession.builder \
        .appName("GenerateADSProductEfficiencyOverview") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.exec.max.dynamic.partitions", "3000") \
        .config("hive.exec.max.dynamic.partitions.pernode", "1000") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE gongdan01")

    # 自动建表（仅第一次有效，存在不会报错）
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_product_efficiency_overview (
            product_id STRING,
            store_id STRING,
            pv BIGINT,
            uv BIGINT,
            avg_stay_time DOUBLE,
            bounce_rate DOUBLE,
            micro_detail_uv BIGINT,
            fav_user_cnt BIGINT,
            cart_user_cnt BIGINT,
            cart_total_qty BIGINT,
            order_user_cnt BIGINT,
            order_total_qty BIGINT,
            order_total_amt DOUBLE,
            pay_user_cnt BIGINT,
            pay_total_qty BIGINT,
            pay_total_amt DOUBLE,
            refund_amt DOUBLE,
            pay_rate DOUBLE,
            order_rate DOUBLE,
            fav_rate DOUBLE,
            cart_rate DOUBLE,
            item_per_pay_user DOUBLE,
            visitor_value DOUBLE
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    # 读取 DWS 层数据
    visit_df = spark.table("dws_product_visit_summary").filter(col("dt") == dt)
    fav_df = spark.table("dws_product_fav_summary").filter(col("dt") == dt)
    cart_df = spark.table("dws_product_cart_summary").filter(col("dt") == dt)
    order_df = spark.table("dws_order_payment_summary").filter(col("dt") == dt)

    # 聚合 + 去重
    overview_df = visit_df.join(fav_df, ["product_id", "store_id", "dt"], "left") \
                          .join(cart_df, ["product_id", "store_id", "dt"], "left") \
                          .join(order_df, ["product_id", "store_id", "dt"], "left") \
                          .groupBy("product_id", "store_id", "dt").agg(
                              _sum("pv").alias("pv"),
                              _sum("uv").alias("uv"),
                              _round(_sum(col("avg_stay_time") * col("uv")) / _sum("uv"), 2).alias("avg_stay_time"),
                              _round(_sum(col("bounce_rate") * col("pv")) / _sum("pv"), 4).alias("bounce_rate"),
                              _sum("micro_detail_uv").alias("micro_detail_uv"),
                              _sum("fav_user_cnt").alias("fav_user_cnt"),
                              _sum("cart_user_cnt").alias("cart_user_cnt"),
                              _sum("cart_total_qty").alias("cart_total_qty"),
                              _sum("order_user_cnt").alias("order_user_cnt"),
                              _sum("order_total_qty").alias("order_total_qty"),
                              _sum("order_total_amt").alias("order_total_amt"),
                              _sum("pay_user_cnt").alias("pay_user_cnt"),
                              _sum("pay_total_qty").alias("pay_total_qty"),
                              _sum("pay_total_amt").alias("pay_total_amt"),
                              _sum("refund_amt").alias("refund_amt")
                          )

    # 指标字段
    overview_df = overview_df.withColumn("pay_rate", when(col("uv") > 0, _round(col("pay_user_cnt") / col("uv"), 4)).otherwise(lit(0.0))) \
                             .withColumn("order_rate", when(col("uv") > 0, _round(col("order_user_cnt") / col("uv"), 4)).otherwise(lit(0.0))) \
                             .withColumn("fav_rate", when(col("uv") > 0, _round(col("fav_user_cnt") / col("uv"), 4)).otherwise(lit(0.0))) \
                             .withColumn("cart_rate", when(col("uv") > 0, _round(col("cart_user_cnt") / col("uv"), 4)).otherwise(lit(0.0))) \
                             .withColumn("item_per_pay_user", when(col("pay_user_cnt") > 0, _round(col("pay_total_amt") / col("pay_user_cnt"), 2)).otherwise(lit(0.0))) \
                             .withColumn("visitor_value", when(col("uv") > 0, _round(col("pay_total_amt") / col("uv"), 2)).otherwise(lit(0.0)))

    # 强制字段顺序对齐表结构
    ordered_df = overview_df.select(
        "product_id",
        "store_id",
        "pv",
        "uv",
        "avg_stay_time",
        "bounce_rate",
        "micro_detail_uv",
        "fav_user_cnt",
        "cart_user_cnt",
        "cart_total_qty",
        "order_user_cnt",
        "order_total_qty",
        "order_total_amt",
        "pay_user_cnt",
        "pay_total_qty",
        "pay_total_amt",
        "refund_amt",
        "pay_rate",
        "order_rate",
        "fav_rate",
        "cart_rate",
        "item_per_pay_user",
        "visitor_value",
        "dt"
    )

    ordered_df.write.mode("overwrite").format("orc").insertInto("ads_product_efficiency_overview")

    print("? ads_product_efficiency_overview 写入成功，分区：{}".format(dt))
    spark.stop()

if __name__ == "__main__":
    dt = sys.argv[1] if len(sys.argv) > 1 else generate_date_string(1)
    print("生成 ads_product_efficiency_overview，分区：{}".format(dt)) 
    generate_ads_overview(dt)
