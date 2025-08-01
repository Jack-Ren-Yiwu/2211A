# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import to_timestamp
from datetime import datetime, timedelta
import random
import uuid

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("HiveOdsSimulateData") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

# 选择数据库
spark.sql("USE gongdan10")

# 工具函数
def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))

def random_position():
    return "{}_{}".format(random.randint(0, 1920), random.randint(0, 1080))

def random_device():
    return random.choice(["iOS", "Android", "PC", "Tablet"])

def random_region():
    return random.choice(["California", "Texas", "Ontario", "Beijing", "Shanghai"])

def random_module_type():
    return random.choice(["Banner", "FlashSale", "Recommendation", "SearchBox", "CategoryGrid"])

# -----------------------
# 1. ods_page_visit_log
# -----------------------
visit_data = []
start_time = datetime(2025, 7, 1)
end_time = datetime(2025, 7, 31)

for i in range(10000):
    visit_data.append((
        str(uuid.uuid4()),
        "user_{:04d}".format(random.randint(1, 500)),
        random_date(start_time, end_time).strftime("%Y-%m-%d %H:%M:%S"),
        "page_{:03d}".format(random.randint(1, 50)),
        "module_{:03d}".format(random.randint(1, 200)),
        random_position(),
        "192.168.{}.{}".format(random.randint(0, 255), random.randint(0, 255)),
        random_device()
    ))

visit_schema = StructType([
    StructField("visit_id", StringType()),
    StructField("user_id", StringType()),
    StructField("visit_time", StringType()),  # 先用StringType，后面转换
    StructField("page_id", StringType()),
    StructField("module_id", StringType()),
    StructField("click_position", StringType()),
    StructField("ip", StringType()),
    StructField("device", StringType())
])

visit_df_raw = spark.createDataFrame(visit_data, visit_schema)
visit_df = visit_df_raw.withColumn("visit_time", to_timestamp("visit_time", "yyyy-MM-dd HH:mm:ss"))

visit_df.write.mode("overwrite").saveAsTable("ods_page_visit_log")

# -----------------------
# 2. ods_page_module_config
# -----------------------
module_data = []
for i in range(1, 201):
    module_data.append((
        "module_{:03d}".format(i),
        "page_{:03d}".format(random.randint(1, 50)),
        "ModuleName_{}".format(i),
        random_module_type(),
        random_position(),
        random.randint(1, 3)
    ))

module_schema = StructType([
    StructField("module_id", StringType()),
    StructField("page_id", StringType()),
    StructField("module_name", StringType()),
    StructField("module_type", StringType()),
    StructField("position", StringType()),
    StructField("display_order", IntegerType())
])

spark.createDataFrame(module_data, module_schema) \
    .write.mode("overwrite").saveAsTable("ods_page_module_config")

# -----------------------
# 3. ods_page_to_product_log
# -----------------------
jump_data = []
for i in range(8000):
    jump_data.append((
        "jump_{:05d}".format(i),
        "user_{:04d}".format(random.randint(1, 500)),
        "page_{:03d}".format(random.randint(1, 50)),
        "module_{:03d}".format(random.randint(1, 200)),
        "product_{:05d}".format(random.randint(1, 1000)),
        random_date(start_time, end_time).strftime("%Y-%m-%d %H:%M:%S")
    ))

jump_schema = StructType([
    StructField("jump_id", StringType()),
    StructField("user_id", StringType()),
    StructField("page_id", StringType()),
    StructField("module_id", StringType()),
    StructField("product_id", StringType()),
    StructField("jump_time", StringType())
])

jump_df_raw = spark.createDataFrame(jump_data, jump_schema)
jump_df = jump_df_raw.withColumn("jump_time", to_timestamp("jump_time", "yyyy-MM-dd HH:mm:ss"))

jump_df.write.mode("overwrite").saveAsTable("ods_page_to_product_log")

# -----------------------
# 4. ods_product_info
# -----------------------
product_data = []
for i in range(1, 1001):
    product_data.append((
        "product_{:05d}".format(i),
        "sku_{:05d}".format(i),
        "SPU_{:04d}".format(random.randint(1, 300)),
        random.choice(["A", "B", "C", "D", "E"]),
        round(random.uniform(10, 1000), 2)
    ))

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("sku_id", StringType()),
    StructField("spu_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType())
])

spark.createDataFrame(product_data, product_schema) \
    .write.mode("overwrite").saveAsTable("ods_product_info")

# -----------------------
# 5. ods_user_info
# -----------------------
user_data = []
for i in range(1, 501):
    user_data.append((
        "user_{:04d}".format(i),
        random_device(),
        random_region(),
        random.choice(["yes", "no"])
    ))

user_schema = StructType([
    StructField("user_id", StringType()),
    StructField("device_type", StringType()),
    StructField("region", StringType()),
    StructField("is_new_user", StringType())
])

spark.createDataFrame(user_data, user_schema) \
    .write.mode("overwrite").saveAsTable("ods_user_info")
