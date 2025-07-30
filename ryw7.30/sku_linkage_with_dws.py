# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit, expr
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

# 切换数据库
spark.sql("USE gongdan")

# 模拟主商品 SKU 和相关 SKU
main_skus = ["main_sku_" + str(i) for i in range(1, 6)]
related_skus = ["related_sku_{}".format(i) for i in range(1, 31)]
all_skus = main_skus + related_skus
users = ["user_{:03d}".format(i) for i in range(1, 51)]
dt = "2025-07-30"

# ---------- 第一步：建表 dws_user_sku_action_day ----------
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS gongdan.dws_user_sku_action_day (
        user_id     STRING COMMENT '用户ID',
        sku_id      STRING COMMENT '商品ID',
        action_type STRING COMMENT '行为类型（view/cart/order）',
        action_time STRING COMMENT '行为时间'
    )
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan/dws_user_sku_action_day'
""")

# ---------- 第二步：生成模拟数据并写入 dws 表 ----------
data = []
for user in users:
    view_times = random.randint(5, 15)
    for _ in range(view_times):
        is_main = random.random() < 0.7
        sku = random.choice(main_skus) if is_main else random.choice(related_skus)
        hour = random.randint(9, 22)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        timestamp = "{} {:02d}:{:02d}:{:02d}".format(dt, hour, minute, second)
        data.append((user, sku, "view", timestamp, dt))

        if random.random() < 0.4:  # 加入购物车
            cart_time = (datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(5, 60))).strftime("%Y-%m-%d %H:%M:%S")
            data.append((user, sku, "cart", cart_time, dt))

            if random.random() < 0.5:  # 下单
                order_time = (datetime.strptime(cart_time, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=random.randint(10, 90))).strftime("%Y-%m-%d %H:%M:%S")
                data.append((user, sku, "order", order_time, dt))

columns = ["user_id", "sku_id", "action_type", "action_time", "dt"]
df_dws = spark.createDataFrame(data, columns)

# 删除旧分区并写入
spark.sql("ALTER TABLE gongdan.dws_user_sku_action_day DROP IF EXISTS PARTITION (dt='{}')".format(dt))
df_dws.write.mode("overwrite").insertInto("gongdan.dws_user_sku_action_day")

# ---------- 第三步：创建 ADS 表 ----------
spark.sql("""
    CREATE EXTERNAL TABLE IF NOT EXISTS gongdan.ads_sku_linkage_stats (
        main_sku_id       STRING COMMENT '主商品ID',
        related_sku_id    STRING COMMENT '连带商品ID',
        visit_user_count  BIGINT  COMMENT '访问人数',
        cart_user_count   BIGINT  COMMENT '加购人数',
        order_user_count  BIGINT  COMMENT '支付人数'
    )
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET
    LOCATION '/warehouse/gongdan/ads_sku_linkage_stats'
""")

# ---------- 第四步：计算连带指标并写入 ADS ----------
df = spark.table("gongdan.dws_user_sku_action_day").filter(col("dt") == dt)

main_view_df = df.filter((col("sku_id").isin(main_skus)) & (col("action_type") == "view")) \
    .select("user_id", col("sku_id").alias("main_sku_id")).distinct()

joined = main_view_df.join(df, on="user_id") \
    .filter(~col("sku_id").isin(main_skus)) \
    .withColumnRenamed("sku_id", "related_sku_id")

result_df = joined.groupBy("main_sku_id", "related_sku_id") \
    .agg(
        countDistinct(expr("CASE WHEN action_type='view' THEN user_id END")).alias("visit_user_count"),
        countDistinct(expr("CASE WHEN action_type='cart' THEN user_id END")).alias("cart_user_count"),
        countDistinct(expr("CASE WHEN action_type='order' THEN user_id END")).alias("order_user_count")
    ) \
    .withColumn("dt", lit(dt))

spark.sql("ALTER TABLE gongdan.ads_sku_linkage_stats DROP IF EXISTS PARTITION (dt='{}')".format(dt))
result_df.write.mode("overwrite").insertInto("gongdan.ads_sku_linkage_stats")

spark.stop
