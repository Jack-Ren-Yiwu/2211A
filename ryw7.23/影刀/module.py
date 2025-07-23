import xbot
from xbot import print, sleep
from . import package
from .package import variables as glv

import re
import requests
from lxml import etree
import pymysql

# 缩写字母国家字典（🇨🇳_中国）
country_dict = {
    '🇨🇳': '中国',
    '🇺🇸': '美国',
    '🇯🇵': '日本',
    '🇭🇰': '香港',
    '🇹🇼': '台湾',
    '🇰🇷': '韩国',
    '🇩🇪': '德国',
    '🇫🇷': '法国',
    '🇮🇹': '意大利',
    '🇪🇸': '西班牙',
    '🇷🇺': '俄罗斯',
    "🇮🇳": '印度',
    "🇬🇧": '英国',
    "🇫🇮": '芬兰',
    "🇱🇧": '黎巴嫩共和国',
    "🇦🇺": '澳大利亚',
}


def get_movie():
    url = 'http://www.boxofficecn.com/the-red-box-office'
    try:
        res = requests.get(url)
        # 检查请求是否成功
        if res.status_code != 200:
            print(f"请求失败，状态码: {res.status_code}")
            return []

        res_text = etree.HTML(res.text)
        # 检查是否成功解析
        if res_text is None:
            print("无法解析网页内容")
            return []

        tr_list = res_text.xpath('//*[@id="tablepress-4"]/tbody/tr')
        result = []
        for tr in tr_list:
            # 检查tr是否有内容
            if not tr.xpath('./td[1]/text()'):
                continue

            movie_time_and_country = tr.xpath('./td[1]/text()')[0]

            try:
                movie_time, movie_country_words = movie_time_and_country.split(' ')
            except Exception as e:
                print(e)
                # 正则识别年份与国家
                movie_time = re.findall(r'\d+', movie_time_and_country)[0]
                movie_country_words = movie_time_and_country.split(movie_time)[1]
                print(movie_time, movie_country_words)

            # 检查国家代码是否存在
            if movie_country_words.strip() not in country_dict:
                print(f"未知国家代码: {movie_country_words.strip()}")
                continue

            movie_country = country_dict[movie_country_words.strip()]

            # 检查电影名称和评分是否存在
            if not tr.xpath('./td[2]/text()'):
                continue

            movie_name_and_score = tr.xpath('./td[2]/text()')[0]
            movie_name = movie_name_and_score.split('（')[0]
            movie_score = movie_name_and_score.split('（')[1].split('）')[0]

            # 检查演员信息是否存在
            movie_actors = tr.xpath('./td[3]/text()')[0] if tr.xpath('./td[3]/text()') else ""

            # 处理票房数据
            movie_money_list = tr.xpath('./td[last()]/font/text()') or tr.xpath('./td[last()]/text()') or []
            if len(movie_money_list) == 0:
                movie_money = 0
            else:
                try:
                    # 移除可能的逗号分隔符
                    money_str = movie_money_list[0].replace(',', '')
                    # 提取数字部分
                    money_num = re.findall(r'\d+\.?\d*', money_str)[0]
                    movie_money = int(float(money_num))
                except Exception as e:
                    print(f"处理票房数据出错: {e}, 数据: {movie_money_list[0]}")
                    movie_money = 0

            # 将数据整合成字典
            movie_dict = {
                'movie_time': movie_time,
                'movie_name': movie_name,
                'movie_score': movie_score,
                'movie_country': movie_country,
                'movie_actors': movie_actors,
                'movie_money': movie_money,
            }
            result.append(movie_dict)
        print(f"成功获取 {len(result)} 条电影数据")
        return result
    except Exception as e:
        print(f"获取电影数据出错: {str(e)}")
        return []


def perfect_data(movie_data, submitter):
    if not movie_data:
        print("没有电影数据需要处理")
        return []

    mysql_insert_data = []
    for movie in movie_data:
        # 确保所有字段都有值，避免None
        movies_tuple = (
            movie["movie_name"] or "",
            movie["movie_time"] or "",
            movie["movie_country"] or "",
            movie["movie_score"] or "0",
            movie["movie_actors"] or "",  # 注意：这里如果数据库字段是导演，可能需要修改数据来源
            movie["movie_money"],
            submitter or ""
        )
        mysql_insert_data.append(movies_tuple)
    print(f"处理完成 {len(mysql_insert_data)} 条插入数据")
    return mysql_insert_data


def insert_movie(movie_data):
    if not movie_data:
        print("没有数据需要插入数据库")
        return

    my_cursor = None
    conn = None
    success_count = 0
    fail_count = 0

    try:
        # 连接数据库
        conn = pymysql.connect(
            host='43.143.30.32',
            user='yingdao',
            password='9527',
            database='ydtest',
            charset='utf8',
            port=3306,  # 添加默认端口，确保连接参数完整
            connect_timeout=10
        )
        my_cursor = conn.cursor()

        # 先查询表结构，确认字段是否匹配（调试用）
        my_cursor.execute("DESCRIBE movie")
        print("表结构信息:")
        for field in my_cursor.fetchall():
            print(field)

        # sql语句 - 确认字段对应关系是否正确
        # 注意：如果"导演"字段应该对应其他数据，请修改
        insert_sql = """
                INSERT INTO movie (电影名称, 上映年份, 制片地区, 评分, 导演, 票房, 提交人)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

        for idx, movie in enumerate(movie_data):
            try:
                # 打印当前插入的数据，方便调试
                print(f"正在插入第 {idx + 1} 条数据: {movie}")
                my_cursor.execute(insert_sql, movie)
                conn.commit()
                success_count += 1
                print(f"第 {idx + 1} 条数据插入成功")
            except pymysql.IntegrityError as e:
                fail_count += 1
                conn.rollback()
                print(f"第 {idx + 1} 条数据插入失败（完整性错误）: {str(e)}, 数据: {movie}")
            except pymysql.DataError as e:
                fail_count += 1
                conn.rollback()
                print(f"第 {idx + 1} 条数据插入失败（数据格式错误）: {str(e)}, 数据: {movie}")
            except Exception as e:
                fail_count += 1
                conn.rollback()
                print(f"第 {idx + 1} 条数据插入失败: {str(e)}, 数据: {movie}")

        print(f"插入完成 - 成功: {success_count}, 失败: {fail_count}")

    except pymysql.OperationalError as e:
        print(f"数据库连接失败: {str(e)}")
        print("请检查主机地址、端口、用户名、密码是否正确")
    except Exception as e:
        print(f"数据库操作出错: {str(e)}")
    finally:
        if my_cursor is not None:
            my_cursor.close()
        if conn is not None:
            conn.close()


def main(*args):
    submitter = "任一午"
    print(f"开始执行电影数据爬取和插入，提交人: {submitter}")
    movies_data = get_movie()
    if not movies_data:
        print("未获取到电影数据，终止流程")
        return

    movies_datas = perfect_data(movies_data, submitter)
    insert_movie(movies_datas)
    print("执行完成")
