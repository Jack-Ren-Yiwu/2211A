# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, round as _round, sum as _sum
import datetime
import sys

def generate_date_string(offset=0):
    return (datetime.datetime.now() - datetime.timedelta(days=offset)).strftime("%Y%m%d")

def generate_ads(dt):
    spark = SparkSession.builder \
        .appName("GenerateADS") \
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

    # 创建 ADS 表
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_product_efficiency_overview (
            product_id INT,
            store_id INT,
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
            order_total_amt DECIMAL(20,2),
            pay_user_cnt BIGINT,
            pay_total_qty BIGINT,
            pay_total_amt DECIMAL(20,2),
            refund_amt DECIMAL(20,2),
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

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_product_efficiency_summary (
            store_id INT,
            pv BIGINT,
            uv BIGINT,
            order_user_cnt BIGINT,
            pay_user_cnt BIGINT,
            order_total_amt DECIMAL(20,2),
            pay_total_amt DECIMAL(20,2),
            dt STRING
        )
        STORED AS ORC
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_product_conversion_rate (
            product_id INT,
            store_id INT,
            uv BIGINT,
            click_uv BIGINT,
            order_uv BIGINT,
            pay_uv BIGINT,
            click_rate DOUBLE,
            order_rate DOUBLE,
            pay_rate DOUBLE,
            dt STRING
        )
        STORED AS ORC
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_product_segment_analysis (
            product_id INT,
            store_id INT,
            uv BIGINT,
            pay_user_cnt BIGINT,
            order_user_cnt BIGINT,
            bounce_rate DOUBLE,
            pay_rate DOUBLE,
            visitor_value DOUBLE,
            conversion_segment STRING,
            pay_segment STRING,
            bounce_segment STRING,
            dt STRING
        )
        STORED AS ORC
    """)

    # 读取 DWS 层数据
    visit_df = spark.table("dws_product_visit_summary").filter(col("dt") == dt)
    fav_df = spark.table("dws_product_fav_summary").filter(col("dt") == dt)
    cart_df = spark.table("dws_product_cart_summary").filter(col("dt") == dt)
    order_df = spark.table("dws_order_payment_summary").filter(col("dt") == dt)

    # 商品效率全景表（聚合后去重）
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

    overview_df = overview_df.withColumn("pay_rate", when(col("uv") > 0, _round(col("pay_user_cnt") / col("uv"), 4)).otherwise(lit(0.0))) \
                             .withColumn("order_rate", when(col("uv") > 0, _round(col("order_user_cnt") / col("uv"), 4)).otherwise(lit(0.0))) \
                             .withColumn("fav_rate", when(col("uv") > 0, _round(col("fav_user_cnt") / col("uv"), 4)).otherwise(lit(0.0))) \
                             .withColumn("cart_rate", when(col("uv") > 0, _round(col("cart_user_cnt") / col("uv"), 4)).otherwise(lit(0.0))) \
                             .withColumn("item_per_pay_user", when(col("pay_user_cnt") > 0, _round(col("pay_total_amt") / col("pay_user_cnt"), 2)).otherwise(lit(0.0))) \
                             .withColumn("visitor_value", when(col("uv") > 0, _round(col("pay_total_amt") / col("uv"), 2)).otherwise(lit(0.0)))

    overview_df.write.mode("overwrite").format("orc").insertInto("ads_product_efficiency_overview")

    # 效率汇总表
    eff_summary_df = overview_df.groupBy("store_id").agg(
        _sum("pv").alias("pv"),
        _sum("uv").alias("uv"),
        _sum("order_user_cnt").alias("order_user_cnt"),
        _sum("pay_user_cnt").alias("pay_user_cnt"),
        _sum("order_total_amt").alias("order_total_amt"),
        _sum("pay_total_amt").alias("pay_total_amt")
    ).withColumn("dt", lit(dt))

    eff_summary_df.write.mode("overwrite").format("orc").insertInto("ads_product_efficiency_summary")

    # 转化率表
    conversion_df = visit_df.join(order_df, ["product_id", "store_id", "dt"], "left") \
        .select(
            col("product_id"),
            col("store_id"),
            col("uv"),
            col("micro_detail_uv").alias("click_uv"),
            col("order_user_cnt").alias("order_uv"),
            col("pay_user_cnt").alias("pay_uv")
        ) \
        .withColumn("click_rate", when(col("uv") > 0, _round(col("click_uv") / col("uv"), 4)).otherwise(lit(0.0))) \
        .withColumn("order_rate", when(col("click_uv") > 0, _round(col("order_uv") / col("click_uv"), 4)).otherwise(lit(0.0))) \
        .withColumn("pay_rate", when(col("order_uv") > 0, _round(col("pay_uv") / col("order_uv"), 4)).otherwise(lit(0.0))) \
        .withColumn("dt", lit(dt))

    conversion_df.write.mode("overwrite").format("orc").insertInto("ads_product_conversion_rate")

    # 分群分析表
    segment_df = overview_df.withColumn("conversion_segment", 
                            when(col("order_rate") >= 0.3, lit("高转化"))
                           .when(col("order_rate") >= 0.1, lit("中转化"))
                           .otherwise(lit("低转化"))) \
        .withColumn("pay_segment", 
            when(col("visitor_value") >= 200, lit("高价值"))
           .when(col("visitor_value") >= 100, lit("中价值"))
           .otherwise(lit("低价值"))) \
        .withColumn("bounce_segment", 
            when(col("bounce_rate") >= 0.5, lit("高跳出"))
           .when(col("bounce_rate") >= 0.2, lit("中跳出"))
           .otherwise(lit("低跳出"))) \
        .select(
            "product_id", "store_id", "uv", "pay_user_cnt", "order_user_cnt", "bounce_rate",
            "pay_rate", "visitor_value", "conversion_segment", "pay_segment", "bounce_segment"
        ) \
        .withColumn("dt", lit(dt))

    segment_df.write.mode("overwrite").format("orc").insertInto("ads_product_segment_analysis")

    spark.stop()

if __name__ == "__main__":
    dt = sys.argv[1] if len(sys.argv) > 1 else generate_date_string(1)
    print("\u2705 正在生成 ADS 分区数据，日期为：{}".format(dt))
    generate_ads(dt)
