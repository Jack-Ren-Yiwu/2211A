# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import random

spark = SparkSession.builder \
    .appName("HiveDwdETL") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.hive.convertMetastoreOrc", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
    .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE gongdan")

now = datetime.now()
recent_7_days = [(now - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(7)]

# SKU 名称成分
adjectives = ["Elegant", "Modern", "Classic", "Sporty", "Casual", "Luxury", "Comfortable", "Durable", "Stylish", "Trendy"]
materials = ["Leather", "Cotton", "Silk", "Wool", "Denim", "Polyester", "Linen", "Velvet", "Suede", "Nylon"]
categories = ["Jacket", "Shoes", "Hat", "Backpack", "T-Shirt", "Jeans", "Watch", "Sunglasses", "Dress", "Scarf"]
colors = ["Red", "Blue", "Green", "Black", "White", "Yellow", "Brown", "Purple", "Gray", "Orange"]

def gen_sku_name():
    return "{} {} {} {}".format(random.choice(adjectives), random.choice(materials), random.choice(categories), random.choice(colors))

# 生成主商品和其他商品 SKU 列表
main_skus = [("sku_1001", gen_sku_name(), 1), ("sku_1002", gen_sku_name(), 1)]
other_skus = [("sku_1003", gen_sku_name(), 0), ("sku_1004", gen_sku_name(), 0), ("sku_1005", gen_sku_name(), 0)]

products = main_skus + other_skus

df_product = spark.createDataFrame(products, ["sku_id", "sku_name", "is_main_sku"])
df_product.write.mode("overwrite").insertInto("gongdan.dim_product")

def random_time(day, start_hour=9, end_hour=22):
    hour = random.randint(start_hour, end_hour)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    return "{} {:02d}:{:02d}:{:02d}".format(day, hour, minute, second)

# 生成日志数据，用户数 50，主 SKU 访问概率 70%
def gen_logs(time_field):
    logs = []
    user_num = 50
    for day in recent_7_days:
        for user_id in range(1, user_num + 1):
            # 主商品访问，70%概率访问
            if random.random() < 0.7:
                main_sku = random.choice(["sku_1001", "sku_1002"])
                logs.append(("user_{}".format(user_id), main_sku, random_time(day, 9, 22)))
            # 其他商品访问次数随机 1~3 次
            other_visits = random.randint(1, 3)
            for _ in range(other_visits):
                other_sku = random.choice(["sku_1003", "sku_1004", "sku_1005"])
                logs.append(("user_{}".format(user_id), other_sku, random_time(day, 9, 22)))
    return spark.createDataFrame(logs, ["user_id", "sku_id", time_field])

# 购物车和支付时间延后，且概率不同
def gen_cart_logs():
    logs = []
    user_num = 50
    for day in recent_7_days:
        for user_id in range(1, user_num + 1):
            # 40%概率产生购物车行为
            if random.random() < 0.4:
                sku = random.choice(["sku_1001", "sku_1002", "sku_1003", "sku_1004", "sku_1005"])
                logs.append(("user_{}".format(user_id), sku, random_time(day, 10, 23)))
    return spark.createDataFrame(logs, ["user_id", "sku_id", "cart_time"])

def gen_pay_logs():
    logs = []
    user_num = 50
    for day in recent_7_days:
        for user_id in range(1, user_num + 1):
            # 20%概率产生支付行为
            if random.random() < 0.2:
                sku = random.choice(["sku_1001", "sku_1002", "sku_1003", "sku_1004", "sku_1005"])
                logs.append(("user_{}".format(user_id), sku, random_time(day, 11, 23)))
    return spark.createDataFrame(logs, ["user_id", "sku_id", "pay_time"])

gen_logs("visit_time").write.mode("overwrite").insertInto("gongdan.dwd_product_visit_log")
gen_cart_logs().write.mode("overwrite").insertInto("gongdan.dwd_product_cart_log")
gen_pay_logs().write.mode("overwrite").insertInto("gongdan.dwd_product_order_log")

print("✅ 模拟数据写入成功（库：gongdan）")
