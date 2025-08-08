# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random

spark = SparkSession.builder \
    .appName("DimTableGeneration") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.catalogImplementation", "hive") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE gongdan02")

# 英文名称生成
def gen_en_word(prefix, i):
    return "{}_{}".format(prefix, i)

# 1. 店铺维度
store_schema = StructType([
    StructField("store_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("province", StringType(), True),
    StructField("city", StringType(), True)
])

store_types = ["flagship", "franchise", "direct", "third-party"]
provinces = ["Ontario", "Quebec", "British Columbia", "Alberta"]
cities = ["Toronto", "Montreal", "Vancouver", "Calgary"]

store_data = []
for i in range(1000):
    store_data.append((
        "S{:04d}".format(i),
        gen_en_word("Store", i),
        random.choice(store_types),
        random.choice(provinces),
        random.choice(cities)
    ))

spark.createDataFrame(store_data, store_schema) \
    .write.mode("overwrite").format("orc").saveAsTable("dim_store")

# 2. 品牌维度
brand_schema = StructType([
    StructField("brand_id", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("origin_country", StringType(), True)
])

countries = ["USA", "Germany", "France", "China", "Japan", "Canada"]
brand_data = []
for i in range(500):
    brand_data.append((
        "B{:04d}".format(i),
        gen_en_word("Brand", i),
        random.choice(countries)
    ))

spark.createDataFrame(brand_data, brand_schema) \
    .write.mode("overwrite").format("orc").saveAsTable("dim_brand")

# 3. 类目维度
category_schema = StructType([
    StructField("cat_id", StringType(), True),
    StructField("cat_name", StringType(), True),
    StructField("level", IntegerType(), True),
    StructField("parent_id", StringType(), True)
])

category_data = []
for i in range(1, 11):
    category_data.append(("C1_{:02d}".format(i), gen_en_word("Category1", i), 1, None))
    for j in range(1, 6):
        parent = "C1_{:02d}".format(i)
        cat2_id = "C2_{}_{}".format(i, j)
        category_data.append((cat2_id, gen_en_word("Category2", j), 2, parent))
        for k in range(1, 5):
            parent2 = cat2_id
            category_data.append((
                "C3_{}_{}_{}".format(i, j, k),
                gen_en_word("Category3", k),
                3,
                parent2
            ))

spark.createDataFrame(category_data, category_schema) \
    .write.mode("overwrite").format("orc").saveAsTable("dim_category")

# 4. 用户维度（扩展）
user_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("province", StringType(), True),
    StructField("city", StringType(), True)
])

genders = ["male", "female"]
user_data = []
for i in range(5000):
    user_data.append((
        "U{:06d}".format(i),
        random.choice(genders),
        random.randint(18, 60),
        random.choice(provinces),
        random.choice(cities)
    ))

spark.createDataFrame(user_data, user_schema) \
    .write.mode("overwrite").format("orc").saveAsTable("dim_user")
