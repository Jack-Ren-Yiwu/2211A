# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum as _sum, avg, when, date_format, current_date, date_sub
from pyspark.sql.functions import lit

def create_and_load_dws():
    spark = SparkSession.builder \
        .appName("GenerateDwsTables") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE gongdan01")

    ### 1. 商品访问汇总
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

    ub = spark.table("dwd_user_behavior_log").filter(col("visit_time") >= date_sub(current_date(), 30))

    visit_summary = ub.groupBy("product_id", "store_id", "dt").agg(
        countDistinct("user_id").alias("uv"),
        _sum(when(col("is_micro_detail") == True, 1).otherwise(0)).alias("micro_detail_uv"),
        countDistinct("log_id").alias("pv"),
        avg("stay_duration").alias("avg_stay_time"),
        (1 - (_sum(when(col("is_bounce") == False, 1).otherwise(0)) / countDistinct("user_id"))).alias("bounce_rate")
    )

    visit_summary.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dws_product_visit_summary")

    ### 2. 商品收藏汇总
    spark.sql("""
        CREATE TABLE IF NOT EXISTS dws_product_fav_summary (
            product_id INT,
            store_id INT,
            fav_user_cnt BIGINT
        )
        PARTITIONED BY (dt STRING)
        STORED AS ORC
    """)

    fav = spark.table("dwd_favorite_log").filter((col("fav_time") >= date_sub(current_date(), 30)) & (~col("is_cancelled")))

    fav_summary = fav.groupBy("product_id", "store_id", "dt").agg(
        countDistinct("user_id").alias("fav_user_cnt")
    )

    fav_summary.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dws_product_fav_summary")

    ### 3. 商品加购汇总
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

    cart = spark.table("dwd_cart_log").filter((col("add_time") >= date_sub(current_date(), 30)) & (~col("is_deleted")))

    cart_summary = cart.groupBy("product_id", "store_id", "dt").agg(
        countDistinct("user_id").alias("cart_user_cnt"),
        _sum("quantity").alias("cart_total_qty")
    )

    cart_summary.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dws_product_cart_summary")

    ### 4. 商品订单&支付汇总（关键表）
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

    # 订单明细
    order_items = spark.table("dwd_order_item").filter(col("dt") >= date_format(date_sub(current_date(), 30), "yyyyMMdd")) \
        .withColumnRenamed("dt", "order_dt")

    # 支付明细（现在含有 product_id 和 store_id）
    payment = spark.table("dwd_payment_info").filter(col("pay_time") >= date_sub(current_date(), 30)) \
        .withColumnRenamed("dt", "pay_dt")

    # 退款信息
    refund = spark.table("dwd_refund_log").filter(col("refund_time") >= date_sub(current_date(), 30)) \
        .withColumnRenamed("dt", "refund_dt")

    # 订单汇总
    order_summary = order_items.groupBy("product_id", "store_id", "order_dt").agg(
        countDistinct("order_id").alias("order_user_cnt"),
        _sum("quantity").alias("order_total_qty"),
        _sum("amount").alias("order_total_amt")
    ).withColumnRenamed("order_dt", "dt")

    # 支付汇总
    pay_summary = payment.groupBy("product_id", "store_id", "pay_dt").agg(
        countDistinct("user_id").alias("pay_user_cnt"),
        _sum("pay_amount").alias("pay_total_amt"),
        _sum(lit(1)).alias("pay_total_qty")  # 若无 quantity 字段，这里可近似表示次数
    ).withColumnRenamed("pay_dt", "dt")

    # 退款汇总
    refund_summary = refund.groupBy("product_id", "store_id", "refund_dt").agg(
        _sum("refund_amount").alias("refund_amt")
    ).withColumnRenamed("refund_dt", "dt")

    # 合并汇总
    final_summary = order_summary.join(pay_summary, ["product_id", "store_id", "dt"], "full_outer") \
                                 .join(refund_summary, ["product_id", "store_id", "dt"], "full_outer") \
                                 .na.fill(0)

    final_summary.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dws_order_payment_summary")

    spark.stop()

if __name__ == "__main__":
    create_and_load_dws()
