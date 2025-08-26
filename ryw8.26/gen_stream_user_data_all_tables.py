# -*- coding: utf-8 -*-
"""
实时持续写入全部10张用户画像相关表的数据（用于 Flink CDC 实时任务）
"""

import random
import uuid
import time
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

province_city_map = {
    '北京市': ['北京市'], '上海市': ['上海市'], '天津市': ['天津市'], '重庆市': ['重庆市'],
    '广东省': ['广州市', '深圳市', '佛山市'], '浙江省': ['杭州市', '宁波市', '温州市'],
    '江苏省': ['南京市', '苏州市'], '山东省': ['济南市', '青岛市'], '四川省': ['成都市'],
    '湖北省': ['武汉市'], '湖南省': ['长沙市'], '福建省': ['福州市', '厦门市'], '陕西省': ['西安市'],
    '辽宁省': ['沈阳市', '大连市'], '河北省': ['石家庄市'], '山西省': ['太原市'], '安徽省': ['合肥市'],
    '江西省': ['南昌市'], '广西壮族自治区': ['南宁市'], '云南省': ['昆明市'], '贵州省': ['贵阳市'],
    '黑龙江省': ['哈尔滨市'], '吉林省': ['长春市'], '海南省': ['海口市'], '内蒙古自治区': ['呼和浩特市'],
    '宁夏回族自治区': ['银川市'], '新疆维吾尔自治区': ['乌鲁木齐市'], '青海省': ['西宁市'], '西藏自治区': ['拉萨市'],
    '甘肃省': ['兰州市'], '香港特别行政区': ['香港'], '澳门特别行政区': ['澳门'], '台湾省': ['台北市']
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
order_statuses = ['SUCCESS', 'CANCEL', 'WAIT_PAY']
refund_types = ['主动退款', '平台退款', '拒收']
refund_reasons = ['不满意', '质量问题', '尺寸不合适']
page_types = ['首页', '直播页', '详情页', '搜索页']

# 累计自增ID
id_state = {
    "user": 10000,
    "device": 10000,
    "order": 50000,
    "refund": 20000,
    "sku": 1000,
    "behavior": 1,
    "category3": 0
}

def random_time(days_back=7):
    now = datetime.now()
    delta = random.randint(0, days_back * 86400)
    return (now - timedelta(seconds=delta)).strftime('%Y-%m-%d %H:%M:%S')

def random_province_city():
    p = random.choice(province_list)
    c = random.choice(province_city_map[p])
    return p, c

def random_category_brand():
    category = random.choice(list(category_brand_map.keys()))
    brand = random.choice(category_brand_map[category])
    return category, brand

def insert_realtime_batch():
    uid = id_state["user"] = id_state["user"] + 1
    did = id_state["device"] = id_state["device"] + 1
    oid = id_state["order"] = id_state["order"] + 1
    sid = id_state["sku"] = id_state["sku"] + 1
    rid = id_state["refund"] = id_state["refund"] + 1

    category, brand = random_category_brand()
    c3_id = f"C3_{brand}旗舰款"
    c2_id = f"C2_{category}"
    c1_id = f"C1_{category}"
    sku_id = f"SKU{sid:04d}"
    spu_id = f"SPU{sid//3:03d}"
    product_name = f"{brand}旗舰款"
    brand_id = f"B_{brand}"

    # 1. user_info
    province, city = random_province_city()
    cursor.execute("INSERT INTO user_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
        uid, random.choice(['M','F']),
        (datetime.now() - timedelta(days=random.randint(18, 60)*365)).strftime('%Y-%m-%d'),
        random.randint(18, 60), province, city,
        random_time(), random.choice(platforms),
        random.choice(['普通会员','金牌会员','钻石会员']), random.randint(0,10000), random.choice([0,0,0,1])
    ))

    # 2. device_info
    cursor.execute("INSERT INTO device_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
        f"D{did:06d}", uid, random.choice(device_types),
        random.choice(oses), brand, "v" + str(random.randint(1,10)),
        random.choice([0,1]), random_time(), random_time()
    ))

    # 3. sku_info
    cursor.execute("INSERT INTO sku_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (
        sku_id, spu_id, product_name, c3_id,
        round(random.uniform(20,3000),2), brand_id,
        random_time(), random.choice(['上架','下架'])
    ))

    # 4. category固定不变

    # 5. user_level（固定不变）
    # 6. province（固定不变）

    # 7. platform_page_type（固定不变）

    # 8. order_info
    cursor.execute("INSERT INTO order_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
        f"O{oid:07d}", uid, sku_id, c3_id,
        random_time(), round(random.uniform(20, 2000), 2),
        random_time(), random.choice(order_statuses),
        round(random.uniform(0, 100), 2), random.choice(payment_channels)
    ))

    # 9. refund_info（概率触发）
    if random.random() < 0.2:
        cursor.execute("INSERT INTO refund_info VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
            f"R{rid:06d}", f"O{oid:07d}", uid, sku_id,
            round(random.uniform(10, 1000),2),
            random.choice(refund_reasons),
            random.choice(refund_types),
            random_time(), random.choice([0, 1])
        ))

    # 10. user_behavior（每条生成2~4条）
    for _ in range(random.randint(2, 4)):
        bid = id_state["behavior"] = id_state["behavior"] + 1
        cursor.execute("INSERT INTO user_behavior VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", (
            uid, random.choice(['view', 'click', 'fav', 'cart', 'pay']),
            random_time(), sku_id, c1_id, c2_id, c3_id,
            "P" + str(random.randint(0,3)),
            random.choice(platforms), str(uuid.uuid4())
        ))

    conn.commit()

if __name__ == '__main__':
    print("⏳ 正在实时写入全部10张表的数据，每批约1用户...")
    while True:
        insert_realtime_batch()
        print("✅ 写入一批数据...")
        time.sleep(random.randint(2, 5))
