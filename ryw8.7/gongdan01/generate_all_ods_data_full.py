# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
import random
from datetime import datetime, timedelta
import uuid

def gen_uuid():
    return str(uuid.uuid4())

spark = SparkSession.builder \
    .appName("GenerateODSData") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE gongdan01")

def save_to_hive(df, table_name):
    df.write.mode("overwrite").saveAsTable(table_name)

# 1. 用户行为日志 ods_user_behavior_log（30,000 条）
data = []
for _ in range(30000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    data.append((
        gen_uuid(), random.randint(1, 5000), gen_uuid(),
        random.choice(["PC", "Mobile", "Pad"]), random.choice(["seo", "cpc", "app", "direct"]), t,
        random.choice(["product_detail", "store_home", "landing"]),
        random.randint(1, 2000), random.randint(1, 300), random.randint(1, 500),
        random.choice([True, False]), random.choice([True, False])
    ))
cols = ["log_id", "user_id", "session_id", "device_type", "channel", "visit_time",
        "page_type", "product_id", "store_id", "stay_duration", "is_bounce", "is_micro_detail"]
save_to_hive(spark.createDataFrame(data, cols), "ods_user_behavior_log")

# 2. 商品浏览日志 ods_product_view_log（20,000 条）
data = []
for _ in range(20000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    data.append((
        gen_uuid(), random.randint(1, 5000), random.randint(1, 2000),
        random.randint(1, 300), t, random.choice(["PC", "Mobile", "Pad"]),
        random.randint(1, 300), random.choice(["home", "search", "recommend"]),
        random.choice([True, False])
    ))
cols = ["view_id", "user_id", "product_id", "store_id", "view_time", "device_type",
        "duration", "referer", "entry_page"]
save_to_hive(spark.createDataFrame(data, cols), "ods_product_view_log")

# 3. 收藏日志 ods_favorite_log（15,000 条）
data = []
for _ in range(15000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    data.append((
        gen_uuid(), random.randint(1, 5000), random.randint(1, 2000),
        random.randint(1, 300), t, random.choice(["PC", "Mobile"]), random.choice([True, False])
    ))
cols = ["fav_id", "user_id", "product_id", "store_id", "fav_time", "device_type", "is_cancelled"]
save_to_hive(spark.createDataFrame(data, cols), "ods_favorite_log")

# 4. 加购日志 ods_cart_log（20,000 条）
data = []
for _ in range(20000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    data.append((
        gen_uuid(), random.randint(1, 5000), random.randint(1, 2000),
        random.randint(1, 300), t, random.randint(1, 5),
        random.choice(["PC", "Mobile"]), random.choice([True, False])
    ))
cols = ["cart_id", "user_id", "product_id", "store_id", "add_time", "quantity", "device_type", "is_deleted"]
save_to_hive(spark.createDataFrame(data, cols), "ods_cart_log")

# 5. 订单主表 ods_order_info（30,000 条）
data = []
for _ in range(30000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    data.append((
        gen_uuid(), random.randint(1, 5000), random.randint(1, 300),
        t, random.choice(["ordered", "pending", "paid", "completed"]),
        random.choice(["PC", "Mobile"]), random.choice(["normal", "groupbuy"]),
        round(random.uniform(10, 3000), 2), random.randint(1, 10), random.choice([True, False])
    ))
cols = ["order_id", "user_id", "store_id", "order_time", "order_status",
        "order_channel", "order_source", "total_amount", "total_quantity", "is_repeat_buyer"]
save_to_hive(spark.createDataFrame(data, cols), "ods_order_info")

# 6. 订单明细 ods_order_item（40,000 条）
data = []
for _ in range(40000):
    data.append((
        gen_uuid(), gen_uuid(), random.randint(1, 2000), gen_uuid(), random.randint(1, 300),
        random.randint(1, 5), round(random.uniform(10, 2000), 2)
    ))
cols = ["order_item_id", "order_id", "product_id", "sku_id", "store_id", "quantity", "amount"]
save_to_hive(spark.createDataFrame(data, cols), "ods_order_item")

# 7. 支付日志 ods_payment_info（20,000 条）
data = []
for _ in range(20000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    data.append((
        gen_uuid(), gen_uuid(), random.randint(1, 5000), t,
        round(random.uniform(10, 3000), 2), random.choice(["PC", "Mobile"]),
        random.choice(["alipay", "wechat"])
    ))
cols = ["payment_id", "order_id", "user_id", "pay_time", "pay_amount", "device_type", "pay_channel"]
save_to_hive(spark.createDataFrame(data, cols), "ods_payment_info")

# 8. 退款日志 ods_refund_log（5,000 条）
data = []
for _ in range(5000):
    t = datetime.now() - timedelta(days=random.randint(0, 30))
    data.append((
        gen_uuid(), gen_uuid(), random.randint(1, 2000), random.randint(1, 5000),
        t, round(random.uniform(5, 500), 2), random.choice(["refund_only", "return_and_refund"]),
        random.choice([True, False])
    ))
cols = ["refund_id", "order_id", "product_id", "user_id", "refund_time",
        "refund_amount", "refund_type", "is_cod"]
save_to_hive(spark.createDataFrame(data, cols), "ods_refund_log")

# 9. 店铺信息 ods_store_info（300 条）
data = []
for i in range(1, 301):
    t = datetime.now() - timedelta(days=random.randint(100, 2000))
    data.append((
        i, "store_{}".format(i), "cat_{}".format(random.randint(1, 20)),
        t, random.randint(1, 5000), round(random.uniform(1.0, 5.0), 2)
    ))
cols = ["store_id", "store_name", "category_id", "open_time", "owner_id", "store_score"]
save_to_hive(spark.createDataFrame(data, cols), "ods_store_info")

# 10. 商品信息 ods_product_info（2,000 条）
data = []
for i in range(1, 2001):
    t = datetime.now() - timedelta(days=random.randint(30, 1000))
    data.append((
        i, "product_{}".format(i), random.randint(1, 300), "cat_{}".format(random.randint(1, 20)),
        round(random.uniform(10, 5000), 2), random.choice(["active", "inactive"]),
        random.choice([True, False]), t
    ))
cols = ["product_id", "product_name", "store_id", "category_id", "price",
        "status", "is_promoted", "create_time"]
save_to_hive(spark.createDataFrame(data, cols), "ods_product_info")

spark.stop()
