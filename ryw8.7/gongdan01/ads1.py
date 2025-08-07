# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, coalesce

def create_ads_product_efficiency_monitor():
    spark = SparkSession.builder \
        .appName("ADSProductEfficiencyMonitor") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE gongdan01")

    # 读取 DWS 表
    visit_df = spark.table("dws_product_visit_summary")
    fav_df = spark.table("dws_product_fav_summary")
    cart_df = spark.table("dws_product_cart_summary")
    order_df = spark.table("dws_order_payment_summary")

    # 多表连接
    joined_df = visit_df.alias("v") \
        .join(fav_df.alias("f"), ["product_id", "store_id", "dt"], "left_outer") \
        .join(cart_df.alias("c"), ["product_id", "store_id", "dt"], "left_outer") \
        .join(order_df.alias("o"), ["product_id", "store_id", "dt"], "left_outer") \
        .select(
            col("v.product_id"),
            col("v.store_id"),
            col("v.dt"),

            # 访问行为
            col("v.pv"),
            col("v.uv"),
            col("v.avg_stay_time"),
            col("v.bounce_rate"),
            col("v.micro_detail_uv"),

            # 收藏行为
            coalesce(col("f.fav_user_cnt"), lit(0)).alias("fav_user_cnt"),

            # 加购行为
            coalesce(col("c.cart_user_cnt"), lit(0)).alias("cart_user_cnt"),
            coalesce(col("c.cart_total_qty"), lit(0)).alias("cart_total_qty"),

            # 订单支付行为
            coalesce(col("o.order_user_cnt"), lit(0)).alias("order_user_cnt"),
            coalesce(col("o.order_total_qty"), lit(0)).alias("order_total_qty"),
            coalesce(col("o.order_total_amt"), lit(0.0)).alias("order_total_amt"),
            coalesce(col("o.pay_user_cnt"), lit(0)).alias("pay_user_cnt"),
            coalesce(col("o.pay_total_qty"), lit(0)).alias("pay_total_qty"),
            coalesce(col("o.pay_total_amt"), lit(0.0)).alias("pay_total_amt"),
            coalesce(col("o.refund_amt"), lit(0.0)).alias("refund_amt")
        )

    # 添加转化率字段
    result_df = joined_df.withColumn(
        "conversion_rate",
        when(col("uv") == 0, lit(0.0)).otherwise(col("pay_user_cnt") / col("uv"))
    )

    # 添加访问区间字段
    result_df = result_df.withColumn(
        "visit_interval",
        when(col("uv") >= 10000, lit("10000+"))
        .when(col("uv") >= 5000, lit("5000-9999"))
        .when(col("uv") >= 1000, lit("1000-4999"))
        .when(col("uv") >= 100, lit("100-999"))
        .otherwise(lit("0-99"))
    )

    # 建表（如不存在）
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ads_product_efficiency_monitor (
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
            conversion_rate DOUBLE,
            visit_interval STRING
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    # 写入 Hive
    result_df.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ads_product_efficiency_monitor")

    spark.stop()

if __name__ == "__main__":
    create_ads_product_efficiency_monitor()
