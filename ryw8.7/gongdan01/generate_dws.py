# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, round as _round, lit
import datetime
import sys

def generate_date_string(offset=0):
    return (datetime.datetime.now() - datetime.timedelta(days=offset)).strftime("%Y%m%d")

def generate_dws(dt):
    spark = SparkSession.builder \
        .appName("GenerateDWS") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("hive.exec.max.dynamic.partitions", "5000") \
        .config("hive.exec.max.dynamic.partitions.pernode", "1000") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE gongdan01")

    ### 1. 创建 DWS 表 ###
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

    ### 生成数据 ###
    num_records = 50000

    base_df = spark.range(0, num_records).selectExpr(
        "CAST(rand()*1000 AS INT) AS product_id",
        "CAST(rand()*50 AS INT) AS store_id",
        "CAST(rand()*1000 + 500 AS BIGINT) AS pv",
        "CAST(rand()*300 + 100 AS BIGINT) AS uv",
        "ROUND(rand()*300 + 30, 2) AS avg_stay_time",
        "ROUND(rand()*0.6, 2) AS bounce_rate",
        "CAST(rand()*300 AS BIGINT) AS micro_detail_uv"
    ).withColumn("dt", lit(dt))

    base_df.write.mode("overwrite").format("orc").insertInto("dws_product_visit_summary")

    fav_df = base_df.withColumn("fav_user_cnt", (col("uv") * (rand()*0.3 + 0.05)).cast("bigint"))
    fav_df.select("product_id", "store_id", "fav_user_cnt", "dt") \
          .write.mode("overwrite").format("orc").insertInto("dws_product_fav_summary")

    cart_df = fav_df.withColumn("cart_user_cnt", (col("fav_user_cnt") * (rand()*0.8 + 0.5)).cast("bigint")) \
                    .withColumn("cart_total_qty", (col("cart_user_cnt") * (rand()*2 + 1)).cast("bigint"))
    cart_df.select("product_id", "store_id", "cart_user_cnt", "cart_total_qty", "dt") \
           .write.mode("overwrite").format("orc").insertInto("dws_product_cart_summary")

    order_df = cart_df.withColumn("order_user_cnt", (col("cart_user_cnt") * (rand()*0.8 + 0.3)).cast("bigint")) \
                      .withColumn("order_total_qty", (col("order_user_cnt") * (rand()*2 + 1)).cast("bigint")) \
                      .withColumn("order_total_amt", _round(col("order_total_qty") * (rand()*30 + 10), 2)) \
                      .withColumn("pay_user_cnt", (col("order_user_cnt") * (rand()*0.9 + 0.6)).cast("bigint")) \
                      .withColumn("pay_total_qty", (col("pay_user_cnt") * (rand()*2 + 1)).cast("bigint")) \
                      .withColumn("pay_total_amt", _round(col("pay_total_qty") * (rand()*25 + 10), 2)) \
                      .withColumn("refund_amt", _round(col("pay_total_amt") * (rand()*0.2), 2))
    order_df.select(
        "product_id", "store_id", "order_user_cnt", "order_total_qty", "order_total_amt",
        "pay_user_cnt", "pay_total_qty", "pay_total_amt", "refund_amt", "dt"
    ).write.mode("overwrite").format("orc").insertInto("dws_order_payment_summary")

    spark.stop()

if __name__ == "__main__":
    dt = sys.argv[1] if len(sys.argv) > 1 else generate_date_string(1)
    print("✅ 当前生成数据的分区日期是：{}".format(dt))
    generate_dws(dt)
