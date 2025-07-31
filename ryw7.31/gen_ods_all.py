# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit
import random
from datetime import datetime, timedelta

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("HiveDwdETL") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

# 当前日期分区
dt = datetime.now().strftime("%Y-%m-%d")

# -------------------
# 1. ods_app_visit_log
# -------------------

schema_visit = StructType([
    StructField("log_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("page_id", StringType(), True),
    StructField("page_name", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("ref_page_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("duration", IntegerType(), True),
    StructField("os_type", StringType(), True),
    StructField("app_version", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("province", StringType(), True),
    StructField("city", StringType(), True),
    StructField("ip", StringType(), True)
])

page_list = [
    ("home", "Home Page", "home"),
    ("search", "Search Page", "search"),
    ("detail", "Product Detail", "detail"),
    ("cart", "Cart Page", "cart"),
    ("order", "Order Page", "order"),
]

os_types = ["Android", "iOS"]
channels = ["organic", "campaign", "ad"]
provinces = ["Beijing", "Shanghai", "Guangdong", "Zhejiang"]
cities = {"Beijing": "Beijing", "Shanghai": "Shanghai", "Guangdong": "Guangzhou", "Zhejiang": "Hangzhou"}

data_visit = []
num_users = 200
sessions_per_user = 5
pages_per_session = 3
now = datetime.now()

log_id_counter = 0
for u in range(num_users):
    user_id = "u{}".format(1000 + u)
    for s in range(sessions_per_user):
        session_id = "s{}_{}".format(user_id, s)
        device_id = "d{}".format(random.randint(1, 1000))
        start_time = now - timedelta(days=random.randint(0, 7), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        for p in range(pages_per_session):
            page = page_list[p % len(page_list)]
            if p == 0:
                ref_page_id = None
            else:
                ref_page_id = page_list[(p - 1) % len(page_list)][0]
            event_type = "stay"  # 固定停留行为
            event_time = start_time + timedelta(seconds=p * 60 + random.randint(0, 30))  # 每页间隔约1分钟
            duration = random.randint(1000, 60000)
            os_type = random.choice(os_types)
            app_version = "v{}.{}.{}".format(random.randint(1, 3), random.randint(0, 9), random.randint(0, 9))
            channel = random.choice(channels)
            province = random.choice(provinces)
            city = cities[province]
            ip = "192.168.{}.{}".format(random.randint(0, 255), random.randint(1, 255))
            log_id = "log{}".format(log_id_counter)
            log_id_counter += 1

            data_visit.append((
                log_id, user_id, session_id, device_id,
                page[0], page[1], page[2], ref_page_id,
                event_type, event_time, duration,
                os_type, app_version, channel,
                province, city, ip
            ))

df_visit = spark.createDataFrame(data_visit, schema_visit).withColumn("dt", lit(dt))
df_visit.write.mode("overwrite").partitionBy("dt").format("parquet").saveAsTable("gongdan09.ods_app_visit_log")


# -------------------
# 2. ods_user_info
# -------------------

schema_user = StructType([
    StructField("user_id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("register_time", TimestampType(), True),
    StructField("province", StringType(), True),
    StructField("city", StringType(), True),
    StructField("vip_level", StringType(), True),
    StructField("user_type", StringType(), True)  # new/active/inactive
])

genders = ["male", "female"]
vip_levels = ["none", "silver", "gold", "platinum"]
user_types = ["new", "active", "inactive"]

data_user = []
num_users = 2000
for i in range(1000, 3000):
    reg_time = now - timedelta(days=random.randint(1, 1000))
    prov = random.choice(provinces)
    city = cities[prov]
    data_user.append((
        "u{}".format(i),
        random.choice(genders),
        random.randint(18, 60),
        reg_time,
        prov,
        city,
        random.choice(vip_levels),
        random.choice(user_types)
    ))

df_user = spark.createDataFrame(data_user, schema_user)
df_user.write.mode("overwrite").format("parquet").saveAsTable("gongdan09.ods_user_info")

# -------------------
# 3. ods_product_info
# -------------------

schema_product = StructType([
    StructField("sku_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price",  FloatType(), True),
    StructField("launch_date", TimestampType(), True),
    StructField("is_new", StringType(), True)  # 'Y' or 'N'
])

categories = ["Electronics", "Clothing", "Home", "Beauty", "Sports"]
brands = ["BrandA", "BrandB", "BrandC", "BrandD"]

data_product = []
for i in range(1, 1001):
    launch = now - timedelta(days=random.randint(0, 365))
    data_product.append((
        "sku_{:05d}".format(i),
        "Product_{}".format(i),
        random.choice(categories),
        random.choice(brands),
        round(random.uniform(10.0, 1000.0), 2),
        launch,
        random.choice(["Y", "N"])
    ))

df_product = spark.createDataFrame(data_product, schema_product)
df_product.write.mode("overwrite").format("parquet").saveAsTable("gongdan09.ods_product_info")

# 结束
print("✅ ods_app_visit_log, ods_user_info, ods_product_info 数据生成并写入完成")

# -------------------
# 4. ods_order_info
# -------------------

schema_order = StructType([
    StructField("order_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("sku_id", StringType(), True),
    StructField("order_amount", FloatType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_time", TimestampType(), True),
    StructField("dt", StringType(), True)
])

order_status_list = ["created", "paid", "shipped", "completed", "cancelled"]

data_order = []
num_orders = 5000
for i in range(num_orders):
    order_id = "order_{:06d}".format(i)
    user_id = "u{}".format(random.randint(1000, 2000))
    sku_id = "sku_{:05d}".format(random.randint(1, 1000))
    order_amount = round(random.uniform(10.0, 1000.0), 2)
    order_status = random.choice(order_status_list)
    order_time = now - timedelta(days=random.randint(0, 30), seconds=random.randint(0, 86400))
    data_order.append((order_id, user_id, sku_id, order_amount, order_status, order_time, dt))

df_order = spark.createDataFrame(data_order, schema_order)
df_order.write.mode("overwrite").partitionBy("dt").format("parquet").saveAsTable("gongdan09.ods_order_info")

print("✅ ods_order_info 数据生成并写入完成")


spark.stop()


