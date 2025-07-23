import xbot
from xbot import print, sleep
from . import package
from .package import variables as glv
import requests
import pymysql


def get_data():
    submitter = "任一午"
    url = 'https://mock.jsont.run/6zA7NH6ciqxNxGYzKO-Zx'
    try:
        res = requests.get(url)
        res.raise_for_status()  # 检查请求是否成功
        movies_data = res.json().get('data', [])
        if not isinstance(movies_data, list):
            print("获取的电影数据不是列表，返回空结果")
            return [[], []]
    except Exception as e:
        print(f"请求数据失败: {e}")
        return [[], []]

    country_box_office = {}
    rating_intervals = {
        '3.0-3.5': 0,
        '9.0-9.5': 0,
        '无评分': 0
    }

    for movie in movies_data:
        # 确保movie是字典，避免非字典类型导致错误
        if not isinstance(movie, dict):
            print(f"无效的电影数据（非字典）: {movie}")
            continue

        # 提取字段并处理异常
        try:
            movie_country = movie.get('制片地区', '未知地区')
            movie_money = int(movie.get('票房', 0))
            movie_rating = movie.get('评分', '无评分')
        except (ValueError, TypeError) as e:
            print(f"解析电影数据失败: {e}，电影数据: {movie}")
            continue

        # 统计国家票房
        if movie_country in country_box_office:
            country_box_office[movie_country] += movie_money
        else:
            country_box_office[movie_country] = movie_money

        # 统计评分区间票房
        if movie_rating == '无评分' or movie_rating == '-':
            rating_intervals['无评分'] += movie_money
        else:
            try:
                rating = float(movie_rating)
                if 3.0 <= rating < 3.5:
                    rating_intervals['3.0-3.5'] += movie_money
                elif 9.0 <= rating < 9.5:
                    rating_intervals['9.0-9.5'] += movie_money
            except ValueError:
                rating_intervals['无评分'] += movie_money

    # 构建结果（确保元素为三元组）
    top_countries = sorted(country_box_office.items(), key=lambda x: x[1], reverse=True)[:3]
    top_country_list = [(submitter, country, total_money) for country, total_money in top_countries]
    final_rating_intervals = [(submitter, interval, total_money) for interval, total_money in rating_intervals.items()]

    return [top_country_list, final_rating_intervals]


def insert_movie(movie_data):
    # 数据库连接参数（请确认正确性）
    db_config = {
        'host': '43.143.30.32',
        'port': 3306,
        'user': 'yingdao',
        'password': '9527',
        'database': 'ydtest',
        'charset': 'utf8'
    }

    conn = None
    my_cursor = None
    try:
        # 建立数据库连接
        conn = pymysql.connect(**db_config)
        my_cursor = conn.cursor()

        # 检查表结构是否匹配（提交人、信息、票房总数需存在）
        insert_sql = """
            INSERT INTO result (提交人, 信息, 票房总数)
            VALUES (%s, %s, %s)
        """

        for movie in movie_data:
            # 校验数据格式：必须是包含3个元素的元组或列表
            if not isinstance(movie, (tuple, list)) or len(movie) != 3:
                print(f"跳过无效数据（格式错误）: {movie}")
                continue

            submitter, info, box_office = movie
            # 确保票房是整数（避免类型错误）
            try:
                box_office = int(box_office)
            except (ValueError, TypeError):
                print(f"跳过无效数据（票房非整数）: {movie}")
                continue

            try:
                my_cursor.execute(insert_sql, (submitter, info, box_office))
                conn.commit()
                print(f"插入成功: {movie}")
            except Exception as e:
                conn.rollback()
                print(f"插入失败: {movie}，错误: {e}")

    except Exception as e:
        print(f"数据库连接/操作失败: {e}")
    finally:
        # 关闭连接
        if my_cursor:
            my_cursor.close()
        if conn:
            conn.close()


def main(*args):  # 修改这里，接受任意参数但不使用
    # 忽略传入的参数，使用固定的提交人
    result_data = get_data()
    # 插入国家票房数据
    insert_movie(result_data[0])
    # 插入评分区间数据
    insert_movie(result_data[1])