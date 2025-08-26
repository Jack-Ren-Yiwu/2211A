# -*- coding: utf-8 -*-
"""
最终版：生成10张表的真实模拟数据，符合用户画像系统需求
"""

import random
import uuid
import pymysql
from datetime import datetime, timedelta

conn = pymysql.connect(
    host='cdh01',
    port=3306,
    user='root',
    password='123456',
    database='gongdan07',
    charset='utf8mb4'
)
cursor = conn.cursor()
now = datetime.now()

# 全国省市映射
province_city_map = {
    '北京市': ['北京市'], '上海市': ['上海市'], '天津市': ['天津市'], '重庆市': ['重庆市'],
    '广东省': ['广州市', '深圳市', '佛山市'], '浙江省': ['杭州市', '宁波市', '温州市'],
    '江苏省': ['南京市', '苏州市'], '山东省': ['济南市', '青岛市'],
    '四川省': ['成都市'], '湖北省': ['武汉市'], '湖南省': ['长沙市'], '福建省': ['福州市', '厦门市'],
    '陕西省': ['西安市'], '辽宁省': ['沈阳市', '大连市'], '河北省': ['石家庄市'], '山西省': ['太原市'],
    '安徽省': ['合肥市'], '江西省': ['南昌市'], '广西壮族自治区': ['南宁市'], '云南省': ['昆明市'],
    '贵州省': ['贵阳市'], '黑龙江省': ['哈尔滨市'], '吉林省': ['长春市'], '海南省': ['海口市'],
    '内蒙古自治区': ['呼和浩特市'], '宁夏回族自治区': ['银川市'], '新疆维吾尔自治区': ['乌鲁木齐市'],
    '青海省': ['西宁市'], '西藏自治区': ['拉萨市'], '甘肃省': ['兰州市'], '香港特别行政区': ['香港'],
    '澳门特别行政区': ['澳门'], '台湾省': ['台北市']
}
province_list = list(province_city_map.keys())

category_brand_map = {
    '电子产品': ['小米', '苹果', '华为'],
    '服装': ['阿迪达斯', '耐克', '新百伦'],
    '母婴': ['好孩子', '贝亲', '启赋'],
    '食品': ['三只松鼠', '百草味', '伊利'],
    '家居': ['宜家', '九牧', '海尔']
}
platforms = ['抖音', '快手', '小红书', '京东', '淘宝', '拼多多', '唯品会', '聚划算', '天猫']
payment_channels = ['微信', '支付宝', '银联', '京东白条']
device_types = ['mobile', 'pc']
oses = ['Android', 'iOS', 'HarmonyOS', 'Windows']
page_types = ['首页', '直播页', '详情页', '搜索页']
refund_reasons = ['不满意', '质量问题', '尺寸不合适']
refund_types = ['主动退款', '平台退款', '拒收']
order_statuses = ['SUCCESS', 'CANCEL', 'WAIT_PAY']

def random_time(days_back=7):
    delta = random.randint(0, days_back * 86400)
    return (now - timedelta(seconds=delta)).strftime('%Y-%m-%d %H:%M:%S')

def random_province_city():
    p = random.choice(province_list)
    c = random.choice(province_city_map[p])
    return p, c

def random_category_brand_product():
    category = random.choice(list(category_brand_map.keys()))
    brand = random.choice(category_brand_map[category])
    product = f"{brand}{random.choice(['旗舰款', '标准版', '轻奢款'])}"
    return category, brand, product

# 插入函数（依次执行）
def insert_user_info(n=10000):
    for i in range(n):
        user_id = i + 1
        gender = random.choice(['M', 'F'])
        age = random.randint(18, 60)
        birthday = (now - timedelta(days=age * 365)).strftime('%Y-%m-%d')
        province, city = random_province_city()
        register_time = random_time()
        channel = random.choice(platforms)
        level = random.choice(['普通会员', '金牌会员', '钻石会员'])
        score = random.randint(0, 10000)
        is_blacklist = random.choice([0, 0, 0, 1])
        cursor.execute("""
            INSERT INTO user_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (user_id, gender, birthday, age, province, city, register_time, channel, level, score, is_blacklist))
    conn.commit()

def insert_province():
    for p in province_list:
        city = province_city_map[p][0]
        temp = f"{random.randint(5, 35)}℃"
        cursor.execute("""
            INSERT INTO province VALUES (%s,%s,%s,%s,%s)
        """, (p, city, '中国地区', temp, '一线'))
    conn.commit()

def insert_sku_info(n=500):
    for i in range(n):
        sku_id = "SKU{:04d}".format(i)
        spu_id = "SPU{:03d}".format(i // 3)
        category, brand, product_name = random_category_brand_product()
        category3_id = "C3_" + product_name
        price = round(random.uniform(10, 3000), 2)
        brand_id = "B_" + brand
        on_sale_date = random_time()
        status = random.choice(['上架', '下架'])
        cursor.execute("""
            INSERT INTO sku_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (sku_id, spu_id, product_name, category3_id, price, brand_id, on_sale_date, status))
    conn.commit()

def insert_category():
    for cat, brands in category_brand_map.items():
        for brand in brands:
            c3_id = "C3_" + brand + "旗舰款"
            c2_id = "C2_" + cat
            c1_id = "C1_" + cat
            cursor.execute("""
                INSERT INTO category VALUES (%s, %s, %s, %s)
            """, (c3_id, brand + "旗舰款", c2_id, c1_id))
    conn.commit()

def insert_user_level():
    levels = [('普通会员', '0-999', '新注册'),
              ('金牌会员', '1000-4999', '中活跃'),
              ('钻石会员', '5000+', '高价值')]
    for l, r, d in levels:
        cursor.execute("""
            INSERT INTO user_level VALUES (%s, %s, %s, %s)
        """, (l, l, r, d))
    conn.commit()

def insert_platform_page_type():
    for i, name in enumerate(page_types):
        cursor.execute("""
            INSERT INTO platform_page_type VALUES (%s, %s, %s, %s)
        """, ("P" + str(i), name, "页面", f"用于{name}展示"))
    conn.commit()

def insert_device_info(n=10000):
    for i in range(n):
        device_id = "D{:06d}".format(i)
        user_id = i + 1
        cursor.execute("""
            INSERT INTO device_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            device_id, user_id,
            random.choice(device_types),
            random.choice(oses),
            random.choice(sum(category_brand_map.values(), [])),
            "v" + str(random.randint(1, 10)),
            random.choice([0, 1]),
            random_time(), random_time()))
    conn.commit()

def insert_order_info(n=20000):
    for i in range(n):
        order_id = "O{:07d}".format(i)
        user_id = random.randint(1, 10000)
        sku_id = "SKU{:04d}".format(random.randint(0, 499))
        cat_id = "C3_" + random.choice(sum(category_brand_map.values(), [])) + "旗舰款"
        cursor.execute("""
            INSERT INTO order_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            order_id, user_id, sku_id, cat_id,
            random_time(), round(random.uniform(20, 2000), 2),
            random_time(), random.choice(order_statuses),
            round(random.uniform(0, 100), 2),
            random.choice(payment_channels)))
    conn.commit()

def insert_refund_info(n=3000):
    for i in range(n):
        refund_id = "R{:06d}".format(i)
        order_id = "O{:07d}".format(random.randint(0, 19999))
        user_id = random.randint(1, 10000)
        sku_id = "SKU{:04d}".format(random.randint(0, 499))
        cursor.execute("""
            INSERT INTO refund_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            refund_id, order_id, user_id, sku_id,
            round(random.uniform(10, 2000), 2),
            random.choice(refund_reasons),
            random.choice(refund_types),
            random_time(),
            random.choice([0, 1])))
    conn.commit()


def insert_user_behavior(n=30000):
    for i in range(n):
        user_id = random.randint(1, 10000)
        behavior = random.choice(['view', 'click', 'fav', 'cart', 'pay'])
        behavior_time = random_time()
        sku_id = "SKU{:04d}".format(random.randint(0, 499))
        category1 = random.choice(list(category_brand_map.keys()))
        brand = random.choice(category_brand_map[category1])
        c3_id = f"C3_{brand}旗舰款"
        c2_id = f"C2_{category1}"
        c1_id = f"C1_{category1}"
        page = "P" + str(random.randint(0, 3))
        platform = random.choice(platforms)
        session_id = str(uuid.uuid4())

        cursor.execute("""
            INSERT INTO user_behavior VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (user_id, behavior, behavior_time, sku_id,
               c1_id, c2_id, c3_id, page, platform, session_id))
    conn.commit()

if __name__ == '__main__':
    insert_user_info()
    insert_sku_info()
    insert_category()
    insert_user_level()
    insert_province()
    insert_platform_page_type()
    insert_device_info()
    insert_order_info()
    insert_refund_info()
    insert_user_behavior()
    print("✅ 全部表数据生成完成")
    cursor.close()
    conn.close()
