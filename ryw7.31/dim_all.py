# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit
import random
from datetime import datetime, timedelta

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("GenerateDimTables") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# 维度表设计和数据模拟

# 1. dim_page_info
schema_page = StructType([
    StructField("page_id", StringType(), True),
    StructField("page_name", StringType(), True),
    StructField("page_type", StringType(), True),
    StructField("module_name", StringType(), True)
])

page_data = [
    ("home", "Home Page", "home", "MainModule"),
    ("search", "Search Page", "search", "SearchModule"),
    ("detail", "Product Detail", "detail", "ProductModule"),
    ("cart", "Cart Page", "cart", "CartModule"),
    ("order", "Order Page", "order", "OrderModule"),
    ("campaign", "Campaign Page", "campaign", "PromotionModule"),
    ("subscribe", "Subscribe Page", "subscribe", "UserModule"),
    ("live", "Live Page", "live", "LiveModule"),
]

df_page = spark.createDataFrame(page_data, schema_page)
df_page.write.mode("overwrite").format("parquet").saveAsTable("gongdan09.dim_page_info")

# 2. dim_user_info
schema_user_dim = StructType([
    StructField("user_id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("province", StringType(), True),
    StructField("register_time", StringType(), True)  # 保持字符串格式方便维度表
])

genders = ["male", "female"]
provinces = ["Beijing", "Shanghai", "Guangdong", "Zhejiang", "Sichuan"]
now = datetime.now()

user_dim_data = []
for i in range(1, 2001):
    reg_date = now - timedelta(days=random.randint(30, 1000))
    user_dim_data.append((
        "u{:04d}".format(i),
        random.choice(genders),
        random.randint(18, 60),
        random.choice(provinces),
        reg_date.strftime("%Y-%m-%d %H:%M:%S")
    ))

df_user_dim = spark.createDataFrame(user_dim_data, schema_user_dim)
df_user_dim.write.mode("overwrite").format("parquet").saveAsTable("gongdan09.dim_user_info")

# 3. dim_device_info
schema_device = StructType([
    StructField("device_id", StringType(), True),
    StructField("os", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("model", StringType(), True)
])

os_list = ["Android", "iOS"]
brands_models = {
    "Xiaomi": ["X10", "X11", "Note9"],
    "Huawei": ["Mate40", "P50", "Nova8"],
    "Apple": ["iPhone12", "iPhone13", "iPhone14"],
    "Samsung": ["S20", "S21", "S22"],
    "OnePlus": ["9Pro", "8T", "Nord"]
}

device_data = []
for i in range(1, 1001):
    brand = random.choice(list(brands_models.keys()))
    model = random.choice(brands_models[brand])
    device_data.append((
        "d{:05d}".format(i),
        random.choice(os_list),
        brand,
        model
    ))

df_device = spark.createDataFrame(device_data, schema_device)
df_device.write.mode("overwrite").format("parquet").saveAsTable("gongdan09.dim_device_info")

# 4. dim_date
schema_date = StructType([
    StructField("date", StringType(), True),
    StructField("week", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("quarter", IntegerType(), True),
    StructField("is_weekend", StringType(), True),
    StructField("is_holiday", StringType(), True)
])

# 生成近3年日期维度
start_date = datetime.now() - timedelta(days=365*3)
date_dim_data = []
holidays = [
    "2023-01-01", "2023-10-01", "2024-01-01", "2024-10-01", "2025-01-01", "2025-10-01"
]  # 简单定义几个假期，可根据需求调整

for i in range(365*3):
    d = start_date + timedelta(days=i)
    date_str = d.strftime("%Y-%m-%d")
    week_num = d.isocalendar()[1]
    month_num = d.month
    quarter_num = (month_num - 1) // 3 + 1
    is_weekend = "Y" if d.weekday() >= 5 else "N"
    is_holiday = "Y" if date_str in holidays else "N"
    date_dim_data.append((
        date_str, week_num, month_num, quarter_num, is_weekend, is_holiday
    ))

df_date = spark.createDataFrame(date_dim_data, schema_date)
df_date.write.mode("overwrite").format("parquet").saveAsTable("gongdan09.dim_date")

print("✅ 维度表 dim_page_info, dim_user_info, dim_device_info, dim_date 生成完毕！")

spark.stop()

