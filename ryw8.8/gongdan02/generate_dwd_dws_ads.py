# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, countDistinct, count, row_number, desc
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Generate_DWD_DWS_ADS_ProductRank") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE gongdan02")

# --------------------
# DWD 层
# --------------------

# 1. DWD订单明细（清洗）
spark.sql("DROP TABLE IF EXISTS dwd_order_detail")
spark.sql("""
    CREATE TABLE dwd_order_detail STORED AS ORC AS
    SELECT
        order_id,
        user_id,
        product_id,
        quantity,
        payment_amount,
        payment_time,
        dt
    FROM ods_order_detail
""")

# 2. DWD商品访问行为（合并访问/收藏/加购）
spark.sql("DROP TABLE IF EXISTS dwd_product_action")
spark.sql("""
    CREATE TABLE dwd_product_action STORED AS ORC AS
    SELECT user_id, product_id, visit_time AS action_time, 'view' AS action_type, dt FROM ods_product_visit_log
    UNION ALL
    SELECT user_id, product_id, fav_time AS action_time, 'fav' AS action_type, dt FROM ods_product_fav_log
    UNION ALL
    SELECT user_id, product_id, cart_time AS action_time, 'cart' AS action_type, dt FROM ods_product_cart_log
""")

# --------------------
# DWS 层：商品汇总
# --------------------
spark.sql("DROP TABLE IF EXISTS dws_product_summary")
spark.sql("""
    CREATE TABLE dws_product_summary STORED AS ORC AS
    SELECT
        od.product_id,
        COUNT(DISTINCT od.user_id) AS pay_user_cnt,
        SUM(od.quantity) AS pay_qty,
        SUM(od.payment_amount) AS pay_amt,
        COUNT(DISTINCT pa.user_id) FILTER(WHERE pa.action_type = 'view') AS view_user_cnt,
        COUNT(DISTINCT pa.user_id) FILTER(WHERE pa.action_type = 'fav') AS fav_user_cnt,
        COUNT(DISTINCT pa.user_id) FILTER(WHERE pa.action_type = 'cart') AS cart_user_cnt,
        dt
    FROM dwd_order_detail od
    LEFT JOIN dwd_product_action pa
        ON od.product_id = pa.product_id AND od.dt = pa.dt
    GROUP BY od.product_id, dt
""")

# --------------------
# ADS 层：商品排行
# --------------------

# 1. 商品销售额排行
spark.sql("DROP TABLE IF EXISTS ads_product_sales_rank")
spark.sql("""
    CREATE TABLE ads_product_sales_rank STORED AS ORC AS
    SELECT
        product_id,
        pay_amt,
        ROW_NUMBER() OVER (PARTITION BY dt ORDER BY pay_amt DESC) AS rank_by_amt,
        dt
    FROM dws_product_summary
""")

# 2. 商品销量排行
spark.sql("DROP TABLE IF EXISTS ads_product_quantity_rank")
spark.sql("""
    CREATE TABLE ads_product_quantity_rank STORED AS ORC AS
    SELECT
        product_id,
        pay_qty,
        ROW_NUMBER() OVER (PARTITION BY dt ORDER BY pay_qty DESC) AS rank_by_qty,
        dt
    FROM dws_product_summary
""")

# 3. 商品转化率排行
spark.sql("DROP TABLE IF EXISTS ads_product_conversion_rank")
spark.sql("""
    CREATE TABLE ads_product_conversion_rank STORED AS ORC AS
    SELECT
        product_id,
        pay_user_cnt,
        view_user_cnt,
        CASE WHEN view_user_cnt > 0 THEN ROUND(pay_user_cnt / view_user_cnt, 4) ELSE 0.0 END AS conversion_rate,
        ROW_NUMBER() OVER (PARTITION BY dt ORDER BY conversion_rate DESC) AS rank_by_conversion,
        dt
    FROM dws_product_summary
""")
