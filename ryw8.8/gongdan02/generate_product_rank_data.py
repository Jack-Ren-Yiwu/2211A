# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round as _round, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import uuid
import random
import datetime

spark = SparkSession.builder \
    .appName("ProductRankDataGeneration") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE gongdan02")

def gen_en_word(prefix, i):
    return "{}_{}".format(prefix, i)

def gen_uuid():
    return str(uuid.uuid4())

def gen_random_datetime():
    now = datetime.datetime.now()
    delta = datetime.timedelta(days=random.randint(0, 29), seconds=random.randint(0, 86399))
    return now - delta

dt_today = datetime.datetime.now().strftime("%Y-%m-%d")

# ------------------ ODS 层 ------------------

# 1. 商品信息表
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("category_level1", StringType(), True),
    StructField("category_level2", StringType(), True),
    StructField("category_level3", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("dt", StringType(), True)
])

product_data = []
for i in range(10000):
    product_data.append((
        "P{:05d}".format(i),
        gen_en_word("Product", i),
        gen_en_word("Brand", random.randint(1, 100)),
        gen_en_word("Cat1", random.randint(1, 10)),
        gen_en_word("Cat2", random.randint(1, 20)),
        gen_en_word("Cat3", random.randint(1, 50)),
        round(random.uniform(10, 1000), 2),
        dt_today
    ))

spark.createDataFrame(product_data, product_schema) \
    .write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ods_product_info")

# 2. 商品访问日志
visit_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("visit_time", TimestampType(), True),
    StructField("device", StringType(), True),
    StructField("dt", StringType(), True)
])

devices = ["mobile", "pc", "tablet"]
visit_data = []
for i in range(10000):
    visit_data.append((
        "U{:06d}".format(random.randint(1, 5000)),
        "P{:05d}".format(random.randint(0, 9999)),
        gen_random_datetime(),
        random.choice(devices),
        dt_today
    ))

spark.createDataFrame(visit_data, visit_schema) \
    .write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ods_product_visit_log")

# 3. 收藏日志
fav_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("fav_time", TimestampType(), True),
    StructField("dt", StringType(), True)
])

fav_data = []
for i in range(10000):
    fav_data.append((
        "U{:06d}".format(random.randint(1, 5000)),
        "P{:05d}".format(random.randint(0, 9999)),
        gen_random_datetime(),
        dt_today
    ))

spark.createDataFrame(fav_data, fav_schema) \
    .write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ods_product_fav_log")

# 4. 加购日志
cart_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("cart_time", TimestampType(), True),
    StructField("dt", StringType(), True)
])

cart_data = []
for i in range(10000):
    cart_data.append((
        "U{:06d}".format(random.randint(1, 5000)),
        "P{:05d}".format(random.randint(0, 9999)),
        gen_random_datetime(),
        dt_today
    ))

spark.createDataFrame(cart_data, cart_schema) \
    .write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ods_product_cart_log")

# 5. 搜索词日志
search_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("search_keyword", StringType(), True),
    StructField("search_time", TimestampType(), True),
    StructField("dt", StringType(), True)
])

keywords = ["shoes", "shirt", "jeans", "jacket", "hat", "bag", "watch", "belt", "glasses", "wallet"]
search_data = []
for i in range(10000):
    search_data.append((
        "U{:06d}".format(random.randint(1, 5000)),
        random.choice(keywords),
        gen_random_datetime(),
        dt_today
    ))

spark.createDataFrame(search_data, search_schema) \
    .write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ods_search_log")

# 6. 订单明细
order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("payment_amount", DoubleType(), True),
    StructField("payment_time", TimestampType(), True),
    StructField("dt", StringType(), True)
])

order_data = []
for i in range(10000):
    qty = random.randint(1, 5)
    price = round(random.uniform(10, 1000), 2)
    order_data.append((
        gen_uuid(),
        "U{:06d}".format(random.randint(1, 5000)),
        "P{:05d}".format(random.randint(0, 9999)),
        qty,
        round(price * qty, 2),
        gen_random_datetime(),
        dt_today
    ))

spark.createDataFrame(order_data, order_schema) \
    .write.mode("overwrite").partitionBy("dt").format("orc").saveAsTable("ods_order_detail")
