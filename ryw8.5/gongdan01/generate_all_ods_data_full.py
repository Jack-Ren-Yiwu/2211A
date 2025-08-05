# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import random
from datetime import datetime, timedelta
import uuid

def gen_uuid():
    return str(uuid.uuid4())

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


# 设定数据库
spark.sql("use gongdan01")

def save_to_hive_auto_create(df, table_name):
    # 自动建表写法，如果表不存在，会自动创建，存在则追加
    df.write.mode("overwrite").saveAsTable(table_name)

# 用户行为日志 ods_user_behavior_log
user_behavior_data = []
for _ in range(30000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    user_behavior_data.append((
        gen_uuid(), random.randint(1, 5000), gen_uuid(), random.choice(["PC", "Mobile", "Pad"]),
        random.choice(["seo", "cpc", "app", "direct"]), t,
        random.choice(["product_detail", "store_home", "landing"]), random.randint(1, 2000),
        random.randint(1, 300), random.randint(1, 500), random.choice([True, False]), random.choice([True, False])
    ))
columns = ["log_id", "user_id", "session_id", "device_type", "channel", "visit_time",
           "page_type", "product_id", "store_id", "stay_duration", "is_bounce", "is_micro_detail"]
df = spark.createDataFrame(user_behavior_data, columns)
save_to_hive_auto_create(df, "ods_user_behavior_log")

# 商品浏览日志 ods_product_view_log
view_data = []
for _ in range(20000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    view_data.append((
        gen_uuid(), random.randint(1, 5000), random.randint(1, 2000),
        random.randint(1, 300), t, random.choice(["PC", "Mobile", "Pad"]),
        random.randint(1, 300), random.choice(["home", "search", "recommend"]), random.choice([True, False])
    ))
columns = ["view_id", "user_id", "product_id", "store_id", "view_time", "device_type",
           "duration", "referer", "entry_page"]
df = spark.createDataFrame(view_data, columns)
save_to_hive_auto_create(df, "ods_product_view_log")

# 收藏日志 ods_favorite_log
favorite_data = []
for _ in range(10000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    favorite_data.append((
        gen_uuid(), random.randint(1, 5000), random.randint(1, 2000),
        random.randint(1, 300), t, random.choice(["PC", "Mobile"]), random.choice([True, False])
    ))
columns = ["fav_id", "user_id", "product_id", "store_id", "fav_time", "device_type", "is_cancelled"]
df = spark.createDataFrame(favorite_data, columns)
save_to_hive_auto_create(df, "ods_favorite_log")

# 加购日志 ods_cart_log
cart_data = []
for _ in range(15000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    cart_data.append((
        gen_uuid(), random.randint(1, 5000), random.randint(1, 2000),
        random.randint(1, 300), t, random.randint(1, 5), random.choice(["PC", "Mobile"]), random.choice([True, False])
    ))
columns = ["cart_id", "user_id", "product_id", "store_id", "add_time", "quantity", "device_type", "is_deleted"]
df = spark.createDataFrame(cart_data, columns)
save_to_hive_auto_create(df, "ods_cart_log")

# 下单主表 ods_order_info
order_info_data = []
for _ in range(12000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    order_info_data.append((
        gen_uuid(), random.randint(1, 5000), random.randint(1, 300),
        t, random.choice(["ordered", "pending", "paid", "completed"]),  # ✅ 中文变英文
        random.choice(["PC", "Mobile"]), random.choice(["normal", "groupbuy"]),  # ✅ 聚划算 -> groupbuy
        round(random.uniform(10, 3000), 2), random.randint(1, 10), random.choice([True, False])
    ))
columns = ["order_id", "user_id", "store_id", "order_time", "order_status",
           "order_channel", "order_source", "total_amount", "total_quantity", "is_repeat_buyer"]
df = spark.createDataFrame(order_info_data, columns)
save_to_hive_auto_create(df, "ods_order_info")
# 订单明细表 ods_order_item
order_item_data = []
for _ in range(20000):
    order_item_data.append((
        gen_uuid(), gen_uuid(), random.randint(1, 2000),
        gen_uuid(), random.randint(1, 300),
        random.randint(1, 5), round(random.uniform(10, 2000), 2)
    ))
columns = ["order_item_id", "order_id", "product_id", "sku_id", "store_id", "quantity", "amount"]
df = spark.createDataFrame(order_item_data, columns)
save_to_hive_auto_create(df, "ods_order_item")

# 支付日志 ods_payment_info
payment_data = []
for _ in range(10000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    payment_data.append((
        gen_uuid(), gen_uuid(), random.randint(1, 5000),
        t, round(random.uniform(10, 3000), 2),
        random.choice(["PC", "Mobile"]), random.choice(["alipay", "wechat"])  # ✅ 支付宝/微信 -> 英文
    ))
columns = ["payment_id", "order_id", "user_id", "pay_time", "pay_amount", "device_type", "pay_channel"]
df = spark.createDataFrame(payment_data, columns)
save_to_hive_auto_create(df, "ods_payment_info")

# 退款日志 ods_refund_log
refund_data = []
for _ in range(3000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    refund_data.append((
        gen_uuid(), gen_uuid(), random.randint(1, 2000),
        random.randint(1, 5000), t, round(random.uniform(5, 500), 2),
        random.choice(["refund_only", "return_and_refund"]),  # ✅ 仅退款、退货退款
        random.choice([True, False])
    ))
columns = ["refund_id", "order_id", "product_id", "user_id", "refund_time", "refund_amount", "refund_type", "is_cod"]
df = spark.createDataFrame(refund_data, columns)
save_to_hive_auto_create(df, "ods_refund_log")

# 店铺信息 ods_store_info
store_data = []
for i in range(1, 301):
    t = datetime.now() - timedelta(days=random.randint(100, 2000))
    store_data.append((i, "store_{}".format(i), "cat_{}".format(random.randint(1, 20)),
                       t, random.randint(1, 5000), round(random.uniform(1.0, 5.0), 2)))
columns = ["store_id", "store_name", "category_id", "open_time", "owner_id", "store_score"]
df = spark.createDataFrame(store_data, columns)
save_to_hive_auto_create(df, "ods_store_info")

# 商品信息 ods_product_info
product_data = []
for i in range(1, 2001):
    t = datetime.now() - timedelta(days=random.randint(30, 1000))
    product_data.append((
        i, "product_{}".format(i), random.randint(1, 300),
        "cat_{}".format(random.randint(1, 20)),
        round(random.uniform(10, 5000), 2),
        random.choice(["active", "inactive"]),  # ✅ 上架 / 下架
        random.choice([True, False]), t
    ))
columns = ["product_id", "product_name", "store_id", "category_id", "price", "status", "is_promoted", "create_time"]
df = spark.createDataFrame(product_data, columns)
save_to_hive_auto_create(df, "ods_product_info")

spark.stop()
