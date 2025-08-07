# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, round as _round, expr, lit, sum as _sum  # Ì†ΩÌ¥ß Ê∑ªÂä† _sum
from pyspark.sql.types import *
import datetime

def generate_date_string(offset=0):
    return (datetime.datetime.now() - datetime.timedelta(days=offset)).strftime("%Y%m%d")

def create_and_load_dws_ads():
    spark = SparkSession.builder \
        .appName("MockDWSandADS") \
        .enableHiveSupport() \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sql("USE gongdan01")

    ### 1. ÂàõÂª∫ DWS Ë°® ###
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dws_product_visit_summary (
            product_id INT,
            store_id INT,
            pv BIGINT,
            uv BIGINT,
            avg_stay_time DOUBLE,
            bounce_rate DOUBLE,
            micro_detail_uv BIGINT
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS dws_product_fav_summary (
            product_id INT,
            store_id INT,
            fav_user_cnt BIGINT
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS dws_product_cart_summary (
            product_id INT,
            store_id INT,
            cart_user_cnt BIGINT,
            cart_total_qty BIGINT
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS dws_order_payment_summary (
            product_id INT,
            store_id INT,
            order_user_cnt BIGINT,
            order_total_qty BIGINT,
            order_total_amt DECIMAL(20,2),
            pay_user_cnt BIGINT,
            pay_total_qty BIGINT,
            pay_total_amt DECIMAL(20,2),
            refund_amt DECIMAL(20,2)
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    ### 2. ÁîüÊàê DWS Êï∞ÊçÆ ###
    num_records = 50000
    dt = generate_date_string(1)

    df = spark.range(0, num_records).selectExpr(
        "CAST(rand()*1000 AS INT) AS product_id",
        "CAST(rand()*50 AS INT) AS store_id",
        "CAST(rand()*1000+100 AS BIGINT) AS pv",
        "CAST(rand()*300+50 AS BIGINT) AS uv",
        "ROUND(rand()*300+30, 2) AS avg_stay_time",
        "ROUND(rand()*0.6, 2) AS bounce_rate",
        "CAST(rand()*200 AS BIGINT) AS micro_detail_uv"
    ).withColumn("dt", lit(dt))

    df.write.mode("overwrite").format("orc").partitionBy("dt").insertInto("dws_product_visit_summary")

    fav_df = df.select("product_id", "store_id", "dt") \
        .withColumn("fav_user_cnt", (col("product_id") % 10 + 5) * 2)
    fav_df.write.mode("overwrite").format("orc").partitionBy("dt").insertInto("dws_product_fav_summary")

    cart_df = df.select("product_id", "store_id", "dt") \
        .withColumn("cart_user_cnt", (col("product_id") % 8 + 10)) \
        .withColumn("cart_total_qty", (col("product_id") % 12 + 20))
    cart_df.write.mode("overwrite").format("orc").partitionBy("dt").insertInto("dws_product_cart_summary")

    order_df = df.select("product_id", "store_id", "dt") \
        .withColumn("order_user_cnt", (col("product_id") % 20 + 1)) \
        .withColumn("order_total_qty", (col("product_id") % 50 + 5)) \
        .withColumn("order_total_amt", _round(rand()*200+50, 2)) \
        .withColumn("pay_user_cnt", (col("product_id") % 15 + 1)) \
        .withColumn("pay_total_qty", (col("product_id") % 30 + 1)) \
        .withColumn("pay_total_amt", _round(rand()*180+40, 2)) \
        .withColumn("refund_amt", _round(rand()*20, 2))
    order_df.write.mode("overwrite").format("orc").partitionBy("dt").insertInto("dws_order_payment_summary")

    ### 3. ÂàõÂª∫ ADS Ë°® ###
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_product_efficiency_summary (
            product_id INT,
            store_id INT,
            product_uv BIGINT,
            micro_detail_uv BIGINT,
            product_pv BIGINT,
            avg_stay_time DOUBLE,
            bounce_rate DOUBLE,
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
            visit_to_fav_rate DOUBLE,
            visit_to_cart_rate DOUBLE,
            visit_to_order_rate DOUBLE,
            visit_to_pay_rate DOUBLE,
            pay_conversion_rate DOUBLE
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_product_conversion_rate (
            product_id INT,
            store_id INT,
            visit_uv BIGINT,
            fav_user_cnt BIGINT,
            cart_user_cnt BIGINT,
            order_user_cnt BIGINT,
            pay_user_cnt BIGINT,
            fav_rate DOUBLE,
            cart_rate DOUBLE,
            order_rate DOUBLE,
            pay_rate DOUBLE
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_store_performance_summary (
            store_id INT,
            order_total_amt DOUBLE,
            pay_total_amt DOUBLE,
            refund_amt DOUBLE,
            visit_uv BIGINT,
            pay_user_cnt BIGINT,
            pay_conversion_rate DOUBLE
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    ### 4. Âä†ËΩΩ DWS Êï∞ÊçÆÔºàÌ†ΩÌ¥ß Áî® format ÊõøÊç¢ f-stringÔºâ ###
    visit = spark.table("dws_product_visit_summary").filter("dt = '{}'".format(dt))
    fav = spark.table("dws_product_fav_summary").filter("dt = '{}'".format(dt))
    cart = spark.table("dws_product_cart_summary").filter("dt = '{}'".format(dt))
    order = spark.table("dws_order_payment_summary").filter("dt = '{}'".format(dt))

    from pyspark.sql.functions import coalesce

    eff_df = visit.alias("v") \
        .join(fav.alias("f"), ["product_id", "store_id", "dt"], "left") \
        .join(cart.alias("c"), ["product_id", "store_id", "dt"], "left") \
        .join(order.alias("o"), ["product_id", "store_id", "dt"], "left") \
        .select(
            col("v.product_id"), col("v.store_id"), col("v.dt"),
            col("v.uv").alias("product_uv"), col("v.micro_detail_uv"),
            col("v.pv").alias("product_pv"), col("v.avg_stay_time"), col("v.bounce_rate"),
            coalesce(col("f.fav_user_cnt"), lit(0)).alias("fav_user_cnt"),
            coalesce(col("c.cart_user_cnt"), lit(0)).alias("cart_user_cnt"),
            coalesce(col("c.cart_total_qty"), lit(0)).alias("cart_total_qty"),
            coalesce(col("o.order_user_cnt"), lit(0)).alias("order_user_cnt"),
            coalesce(col("o.order_total_qty"), lit(0)).alias("order_total_qty"),
            coalesce(col("o.order_total_amt"), lit(0)).alias("order_total_amt"),
            coalesce(col("o.pay_user_cnt"), lit(0)).alias("pay_user_cnt"),
            coalesce(col("o.pay_total_qty"), lit(0)).alias("pay_total_qty"),
            coalesce(col("o.pay_total_amt"), lit(0)).alias("pay_total_amt"),
            coalesce(col("o.refund_amt"), lit(0)).alias("refund_amt")
        ) \
        .withColumn("visit_to_fav_rate", col("fav_user_cnt") / col("product_uv")) \
        .withColumn("visit_to_cart_rate", col("cart_user_cnt") / col("product_uv")) \
        .withColumn("visit_to_order_rate", col("order_user_cnt") / col("product_uv")) \
        .withColumn("visit_to_pay_rate", col("pay_user_cnt") / col("product_uv")) \
        .withColumn("pay_conversion_rate", col("pay_user_cnt") / col("order_user_cnt"))

    eff_df.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ads_product_efficiency_summary")

    conv_df = eff_df.select(
        "product_id", "store_id", "dt", "product_uv",
        "fav_user_cnt", "cart_user_cnt", "order_user_cnt", "pay_user_cnt"
    ).withColumn("fav_rate", col("fav_user_cnt") / col("product_uv")) \
     .withColumn("cart_rate", col("cart_user_cnt") / col("product_uv")) \
     .withColumn("order_rate", col("order_user_cnt") / col("product_uv")) \
     .withColumn("pay_rate", col("pay_user_cnt") / col("product_uv")) \
     .withColumnRenamed("product_uv", "visit_uv")

    conv_df.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ads_product_conversion_rate")

    store_df = eff_df.groupBy("store_id", "dt").agg(
        _sum("order_total_amt").alias("order_total_amt"),
        _sum("pay_total_amt").alias("pay_total_amt"),
        _sum("refund_amt").alias("refund_amt"),
        _sum("product_uv").alias("visit_uv"),
        _sum("pay_user_cnt").alias("pay_user_cnt")
    ).withColumn("pay_conversion_rate", col("pay_user_cnt") / col("visit_uv"))

    store_df.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ads_store_performance_summary")

    spark.stop()

if __name__ == "__main__":
    create_and_load_dws_ads()
