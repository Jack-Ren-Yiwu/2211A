import requests   #发送网页请求
from bs4 import BeautifulSoup #解析网页 HTML
import pandas as pd #用于保存为表格（如 CSV）
import  time  #延时，防止被豆瓣封掉
# 请求头，伪装浏览器访问
headers={
"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}
# 存储所有电影信息
movies=[]
# 爬取每一页（每页25条，共10页）
for start in range(0,250,25):
    #构造请求地址，start 参数控制页码。
    url=f"https://movie.douban.com/top250?start={start}&filter="
    #向豆瓣发出 GET 请求，请求网页源码。
    response=requests.get(url,headers=headers)
    #用 BeautifulSoup 解析返回的 HTML 页面。
    soup=BeautifulSoup(response.text,"html.parser")
#每一部电影的信息都在 <div class="item"> 里，这里找到所有电影条目。
    items=soup.find_all("div",class_="item")
    #遍历这一页的所有电影。
    for item in items:
        #电影名称
        title=item.find("span",class_="title").text
        # 排名
        rank=item.find("em").text
        #导演和主演信息（带换行，先 .strip() 去空格、.replace() 清理格式）
        info=item.find("div",class_="bd").p.text.strip().replace("\n","").replace(" "," ")
        #豆瓣评分
        rating=item.find("span",class_="rating_num").text
        #精彩短评（有的电影没有短评，所以用 if quote_tag else "" 处理）
        quote_tag=item.find("span",class_="inq")
        quote = quote_tag.text if quote_tag else ""
        #评价人数（例如 "2000000人评价" → 去掉 "人评价"）
        people = item.find_all("span")[-2].text.replace("人评价", "")
        # 把每部电影信息打包成字典，添加进 movies 列表中。
        movies.append({
            "排名": rank,
            "电影名称": title,
            "详情": info,
            "评分": rating,
            "评价人数": people,
            "短评": quote
        })

        print(f"已爬取第 {start // 25 + 1} 页")
        time.sleep(1)  # 防止反爬，延时1秒

    # 保存为CSV文件
    #把所有电影数据转换为 DataFrame 表格。
    df = pd.DataFrame(movies)
    df.to_csv(r"D:\PythonProject\zhuangao5\豆瓣电影Top250.csv", index=False, encoding='utf-8-sig')
    print("✅ 爬取完成，结果保存在：豆瓣电影Top250.csv")
































