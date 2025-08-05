# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, current_date, date_sub, current_timestamp

def create_and_load_dwd_with_dim():
    spark = SparkSession.builder \
    .appName("GenerateDwsTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.exec.charset", "UTF-8") \
    .config("spark.hadoop.hive.default.charset", "UTF-8") \
    .config("spark.hadoop.hive.query.result.charset", "UTF-8") \
    .config("spark.sql.session.charset", "UTF-8") \
    .config("spark.executorEnv.LANG", "zh_CN.UTF-8") \
    .config("spark.driverEnv.LANG", "zh_CN.UTF-8") \
    .enableHiveSupport() \
    .getOrCreate()

    spark.sql("USE gongdan01")

    spark.sql('''
    CREATE TABLE IF NOT EXISTS dwd_user_behavior_log (
        log_id STRING,
        user_id INT,
        session_id STRING,
        device_type STRING,
        channel STRING,
        visit_time TIMESTAMP,
        page_type STRING,
        product_id INT,
        product_name STRING,
        store_id INT,
        store_name STRING,
        stay_duration INT,
        is_bounce BOOLEAN,
        is_micro_detail BOOLEAN
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    ''')

    spark.sql('''
    CREATE TABLE IF NOT EXISTS dwd_product_view_log (
        view_id STRING,
        user_id INT,
        product_id INT,
        product_name STRING,
        store_id INT,
        store_name STRING,
        view_time TIMESTAMP,
        device_type STRING,
        duration INT,
        referer STRING,
        entry_page BOOLEAN
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    ''')

    spark.sql('''
    CREATE TABLE IF NOT EXISTS dwd_favorite_log (
        fav_id STRING,
        user_id INT,
        product_id INT,
        product_name STRING,
        store_id INT,
        store_name STRING,
        fav_time TIMESTAMP,
        device_type STRING,
        is_cancelled BOOLEAN
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    ''')

    spark.sql('''
    CREATE TABLE IF NOT EXISTS dwd_cart_log (
        cart_id STRING,
        user_id INT,
        product_id INT,
        product_name STRING,
        store_id INT,
        store_name STRING,
        add_time TIMESTAMP,
        quantity INT,
        device_type STRING,
        is_deleted BOOLEAN
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    ''')

    spark.sql('''
    CREATE TABLE IF NOT EXISTS dwd_order_info (
        order_id STRING,
        user_id INT,
        store_id INT,
        store_name STRING,
        order_time TIMESTAMP,
        order_status STRING,
        order_channel STRING,
        order_source STRING,
        total_amount DECIMAL(10,2),
        total_quantity INT,
        is_repeat_buyer BOOLEAN
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    ''')

    spark.sql('''
    CREATE TABLE IF NOT EXISTS dwd_order_item (
        order_item_id STRING,
        order_id STRING,
        product_id INT,
        product_name STRING,
        sku_id STRING,
        store_id INT,
        store_name STRING,
        quantity INT,
        amount DECIMAL(10,2)
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    ''')

    spark.sql('''
    CREATE TABLE IF NOT EXISTS dwd_payment_info (
        payment_id STRING,
        order_id STRING,
        user_id INT,
        pay_time TIMESTAMP,
        pay_amount DECIMAL(10,2),
        device_type STRING,
        pay_channel STRING
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    ''')

    spark.sql('''
    CREATE TABLE IF NOT EXISTS dwd_refund_log (
        refund_id STRING,
        order_id STRING,
        product_id INT,
        product_name STRING,
        user_id INT,
        refund_time TIMESTAMP,
        refund_amount DECIMAL(10,2),
        refund_type STRING,
        is_cod BOOLEAN
    )
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    ''')

    dim_product = spark.table("dim_product").select(
        col("product_id").alias("dp_product_id"),
        col("product_name")
    )
    dim_store = spark.table("dim_store").select(
        col("store_id").alias("ds_store_id"),
        col("store_name")
    )

    # dwd_user_behavior_log
    ods_ub = spark.table("ods_user_behavior_log")
    dwd_ub = ods_ub.join(dim_product, ods_ub.product_id == dim_product.dp_product_id, "left") \
                   .join(dim_store, ods_ub.store_id == dim_store.ds_store_id, "left") \
                   .select(
                       col("log_id"),
                       col("user_id").cast("int"),
                       col("session_id"),
                       col("device_type"),
                       col("channel"),
                       col("visit_time"),
                       col("page_type"),
                       col("product_id").cast("int"),
                       col("product_name"),
                       col("store_id").cast("int"),
                       col("store_name"),
                       col("stay_duration"),
                       col("is_bounce"),
                       col("is_micro_detail"),
                       date_format(col("visit_time"), "yyyyMMdd").alias("dt")
                   ).filter(col("visit_time") >= date_sub(current_date(), 30))
    dwd_ub.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dwd_user_behavior_log")

    # dwd_product_view_log
    ods_pv = spark.table("ods_product_view_log")
    dwd_pv = ods_pv.join(dim_product, ods_pv.product_id == dim_product.dp_product_id, "left") \
                   .join(dim_store, ods_pv.store_id == dim_store.ds_store_id, "left") \
                   .select(
                       col("view_id"),
                       col("user_id").cast("int"),
                       col("product_id").cast("int"),
                       col("product_name"),
                       col("store_id").cast("int"),
                       col("store_name"),
                       col("view_time"),
                       col("device_type"),
                       col("duration"),
                       col("referer"),
                       col("entry_page"),
                       date_format(col("view_time"), "yyyyMMdd").alias("dt")
                   ).filter(col("view_time") >= date_sub(current_date(), 30))
    dwd_pv.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dwd_product_view_log")

    # dwd_favorite_log
    ods_fav = spark.table("ods_favorite_log")
    dwd_fav = ods_fav.join(dim_product, ods_fav.product_id == dim_product.dp_product_id, "left") \
                     .join(dim_store, ods_fav.store_id == dim_store.ds_store_id, "left") \
                     .select(
                         col("fav_id"),
                         col("user_id").cast("int"),
                         col("product_id").cast("int"),
                         col("product_name"),
                         col("store_id").cast("int"),
                         col("store_name"),
                         col("fav_time"),
                         col("device_type"),
                         col("is_cancelled"),
                         date_format(col("fav_time"), "yyyyMMdd").alias("dt")
                     ).filter(col("fav_time") >= date_sub(current_date(), 30))
    dwd_fav.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dwd_favorite_log")

    # dwd_cart_log
    ods_cart = spark.table("ods_cart_log")
    dwd_cart = ods_cart.join(dim_product, ods_cart.product_id == dim_product.dp_product_id, "left") \
                       .join(dim_store, ods_cart.store_id == dim_store.ds_store_id, "left") \
                       .select(
                           col("cart_id"),
                           col("user_id").cast("int"),
                           col("product_id").cast("int"),
                           col("product_name"),
                           col("store_id").cast("int"),
                           col("store_name"),
                           col("add_time"),
                           col("quantity"),
                           col("device_type"),
                           col("is_deleted"),
                           date_format(col("add_time"), "yyyyMMdd").alias("dt")
                       ).filter(col("add_time") >= date_sub(current_date(), 30))
    dwd_cart.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dwd_cart_log")

    # dwd_order_info
    ods_order = spark.table("ods_order_info")
    dwd_order = ods_order.join(dim_store, ods_order.store_id == dim_store.ds_store_id, "left") \
                         .select(
                             col("order_id"),
                             col("user_id").cast("int"),
                             col("store_id").cast("int"),
                             col("store_name"),
                             col("order_time"),
                             col("order_status"),
                             col("order_channel"),
                             col("order_source"),
                             col("total_amount"),
                             col("total_quantity"),
                             col("is_repeat_buyer"),
                             date_format(col("order_time"), "yyyyMMdd").alias("dt")
                         ).filter(col("order_time") >= date_sub(current_date(), 30))
    dwd_order.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dwd_order_info")

    # dwd_order_item
    ods_order_item = spark.table("ods_order_item")
    dwd_order_item = ods_order_item.join(dim_product, ods_order_item.product_id == dim_product.dp_product_id, "left") \
                                   .join(dim_store, ods_order_item.store_id == dim_store.ds_store_id, "left") \
                                   .select(
                                       col("order_item_id"),
                                       col("order_id"),
                                       col("product_id").cast("int"),
                                       col("product_name"),
                                       col("sku_id"),
                                       col("store_id").cast("int"),
                                       col("store_name"),
                                       col("quantity"),
                                       col("amount"),
                                       date_format(current_timestamp(), "yyyyMMdd").alias("dt")
                                   )
    dwd_order_item.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dwd_order_item")

    # dwd_payment_info
    ods_payment = spark.table("ods_payment_info")
    dwd_payment = ods_payment.select(
        col("payment_id"),
        col("order_id"),
        col("user_id").cast("int"),
        col("pay_time"),
        col("pay_amount"),
        col("device_type"),
        col("pay_channel"),
        date_format(col("pay_time"), "yyyyMMdd").alias("dt")
    ).filter(col("pay_time") >= date_sub(current_date(), 30))
    dwd_payment.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dwd_payment_info")

    # dwd_refund_log
    ods_refund = spark.table("ods_refund_log")
    dwd_refund = ods_refund.join(dim_product, ods_refund.product_id == dim_product.dp_product_id, "left") \
                       .select(
                           col("refund_id"),
                           col("order_id"),
                           col("product_id").cast("int"),
                           col("product_name"),
                           col("user_id").cast("int"),
                           col("refund_time"),
                           col("refund_amount"),
                           col("refund_type"),
                           col("is_cod"),
                           date_format(col("refund_time"), "yyyyMMdd").alias("dt")
                       ).filter(col("refund_time") >= date_sub(current_date(), 30))
    dwd_refund.write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("dwd_refund_log")

    spark.stop()

if __name__ == "__main__":
    create_and_load_dwd_with_dim()