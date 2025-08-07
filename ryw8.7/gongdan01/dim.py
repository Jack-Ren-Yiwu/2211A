# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import random
from datetime import datetime, timedelta
import uuid

def gen_uuid():
    return str(uuid.uuid4())

def generate_dim_date(start_year=2021, end_year=2025):
    data = []
    current = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    while current <= end:
        date_key = current.strftime("%Y%m%d")
        day = current.day
        month = current.month
        year = current.year
        week_of_year = current.isocalendar()[1]
        is_weekend = 1 if current.weekday() >= 5 else 0
        quarter = (month - 1) // 3 + 1
        data.append((date_key, current.date(), day, month, year, week_of_year, is_weekend, quarter))
        current += timedelta(days=1)
    return data

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DimTablesCreateAndLoad") \
        .config("hive.metastore.uris", "thrift://cdh01:9083") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.sql.catalogImplementation", "hive") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("USE gongdan01")

    # --------- 建表语句 ---------
    spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_key STRING,
        date DATE,
        day INT,
        month INT,
        year INT,
        week_of_year INT,
        is_weekend INT,
        quarter INT
    )
    STORED AS ORC
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_store (
        store_id INT,
        store_name STRING,
        category_id STRING,
        owner_id INT,
        open_time DATE,
        store_score FLOAT
    )
    STORED AS ORC
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_product (
        product_id INT,
        product_name STRING,
        store_id INT,
        category_id STRING,
        price FLOAT,
        status STRING,
        is_promoted INT,
        create_time DATE
    )
    STORED AS ORC
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_user (
        user_id INT,
        user_type STRING,
        is_repeat_buyer INT,
        register_date DATE
    )
    STORED AS ORC
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_device (
        device_type STRING,
        channel STRING
    )
    STORED AS ORC
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_order_channel (
        order_channel STRING,
        order_source STRING
    )
    STORED AS ORC
    """)

    spark.sql("""
    CREATE TABLE IF NOT EXISTS dim_page_type (
        page_type STRING
    )
    STORED AS ORC
    """)

    # --------- 生成数据并写入 ---------
    def save_to_hive_auto_create(df, table_name):
        df.write.mode("overwrite").insertInto(table_name, overwrite=True)

    # dim_date
    dim_date_data = generate_dim_date()
    dim_date_schema = StructType([
        StructField("date_key", StringType(), False),
        StructField("date", DateType(), False),
        StructField("day", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("year", IntegerType(), False),
        StructField("week_of_year", IntegerType(), False),
        StructField("is_weekend", IntegerType(), False),
        StructField("quarter", IntegerType(), False)
    ])
    dim_date_df = spark.createDataFrame(dim_date_data, dim_date_schema)
    save_to_hive_auto_create(dim_date_df, "dim_date")

    # dim_store
    store_data = []
    for i in range(1, 301):
        open_time = datetime.now() - timedelta(days=random.randint(100, 2000))
        store_data.append((
            i,
            "store_{}".format(i),
            "cat_{}".format(random.randint(1, 20)),
            random.randint(1, 5000),
            open_time.date(),
            round(random.uniform(1.0, 5.0), 2)
        ))
    store_schema = StructType([
        StructField("store_id", IntegerType(), False),
        StructField("store_name", StringType(), False),
        StructField("category_id", StringType(), False),
        StructField("owner_id", IntegerType(), False),
        StructField("open_time", DateType(), False),
        StructField("store_score", FloatType(), False)
    ])
    dim_store_df = spark.createDataFrame(store_data, store_schema)
    save_to_hive_auto_create(dim_store_df, "dim_store")

    # dim_product
    product_data = []
    for i in range(1, 2001):
        create_time = datetime.now() - timedelta(days=random.randint(30, 1000))
        is_promoted = 1 if random.choice([True, False]) else 0
        product_data.append((
            i,
            "product_{}".format(i),
            random.randint(1, 300),
            "cat_{}".format(random.randint(1, 20)),
            round(random.uniform(10, 5000), 2),
            random.choice(["active", "inactive"]),
            is_promoted,
            create_time.date()
        ))
    product_schema = StructType([
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), False),
        StructField("store_id", IntegerType(), False),
        StructField("category_id", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("status", StringType(), False),
        StructField("is_promoted", IntegerType(), False),
        StructField("create_time", DateType(), False)
    ])
    dim_product_df = spark.createDataFrame(product_data, product_schema)
    save_to_hive_auto_create(dim_product_df, "dim_product")

    # dim_user
    user_data = []
    for i in range(1, 5001):
        user_type = random.choice(["new", "old"])
        is_repeat_buyer = 1 if random.choice([True, False]) else 0
        register_date = datetime.now() - timedelta(days=random.randint(365, 2000))
        user_data.append((
            i,
            user_type,
            is_repeat_buyer,
            register_date.date()
        ))
    user_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("user_type", StringType(), False),
        StructField("is_repeat_buyer", IntegerType(), False),
        StructField("register_date", DateType(), False)
    ])
    dim_user_df = spark.createDataFrame(user_data, user_schema)
    save_to_hive_auto_create(dim_user_df, "dim_user")

    # dim_device
    device_data = []
    device_types = ["PC", "Mobile", "Pad"]
    channels = ["seo", "cpc", "direct", "app"]
    for dt in device_types:
        for ch in channels:
            device_data.append((dt, ch))
    device_schema = StructType([
        StructField("device_type", StringType(), False),
        StructField("channel", StringType(), False)
    ])
    dim_device_df = spark.createDataFrame(device_data, device_schema)
    save_to_hive_auto_create(dim_device_df, "dim_device")

    # dim_order_channel
    order_channel_data = []
    order_channels = ["PC", "Mobile"]
    order_sources = ["normal", "groupbuy"]
    for oc in order_channels:
        for osrc in order_sources:
            order_channel_data.append((oc, osrc))
    order_channel_schema = StructType([
        StructField("order_channel", StringType(), False),
        StructField("order_source", StringType(), False)
    ])
    dim_order_channel_df = spark.createDataFrame(order_channel_data, order_channel_schema)
    save_to_hive_auto_create(dim_order_channel_df, "dim_order_channel")

    # dim_page_type
    page_type_data = [("product_detail",), ("store_home",), ("landing",)]
    page_type_schema = StructType([StructField("page_type", StringType(), False)])
    dim_page_type_df = spark.createDataFrame(page_type_data, page_type_schema)
    save_to_hive_auto_create(dim_page_type_df, "dim_page_type")

    spark.stop()
