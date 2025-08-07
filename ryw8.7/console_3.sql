create database if not exists test_sql;
use test_sql;-- 一些语句会走 MapReduce，所以慢。 可以开启本地化执行的优化。
set hive.exec.mode.local.auto=true;
-- (默认为false)
--第1题：访问量统计
CREATE TABLE test_sql.test1
(
    userId     string,
    visitDate  string,
    visitCount INT
)
    ROW format delimited FIELDS TERMINATED BY "\t";
INSERT overwrite TABLE test_sql.test1
VALUES ('u01', '2017/1/21', 5),
       ('u02', '2017/1/23', 6),
       ('u03', '2017/1/22', 8),
       ('u04', '2017/1/20', 3),
       ('u01', '2017/1/23', 6),
       ('u01', '2017/2/21', 8),
       ('u02', '2017/1/23', 6),
       ('u01', '2017/2/22', 4);


with temp as (select *,
                     sum(visitCount)
                         over (partition by userId order by visitDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) as qa
              from test1)
select userId, date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM'), sum(visitCount), max(qa)
from temp
group by userId, date_format(regexp_replace(visitDate, '/', '-'), 'yyyy-MM');



select *,
       sum(sum1) over (partition by userid order by month1 rows between unbounded
           preceding and current row ) as `累积`
from (select userid,
             date_format(replace(visitdate, '/', '-'), 'yyyy-MM') as month1,
             sum(visitcount)                                         sum1
      from test_sql.test1
      group by userid,
               date_format(replace(visitdate, '/', '-'), 'yyyy-MM')) as t;


-- 第2题：电商场景TopK统计
CREATE TABLE test_sql.test2
(
    user_id string,
    shop    string
)
    ROW format delimited FIELDS TERMINATED BY '\t';
INSERT INTO TABLE test_sql.test2
VALUES ('u1', 'a'),
       ('u2', 'b'),
       ('u1', 'b'),
       ('u1', 'a'),
       ('u3', 'c'),
       ('u4', 'b'),
       ('u1', 'a'),
       ('u2', 'c'),
       ('u5', 'b'),
       ('u4', 'b'),
       ('u6', 'c'),
       ('u2', 'c'),
       ('u1', 'b'),
       ('u2', 'a'),
       ('u2', 'a'),
       ('u3', 'a'),
       ('u5', 'a'),
       ('u5', 'a'),
       ('u5', 'a');

--（1）每个店铺的UV（访客数）-- UV和PV-- PV是访问当前网站所有的次数-- UV是访问当前网站的客户数(需要去重)

select shop, count(DISTINCT user_id)
from test2
group by shop;
--(2)每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
with temp2 as (select *, row_number() over (partition by shop order by count(user_id)) as pd
               from test2
               group by shop, user_id)
select *
from temp2
where pd <= 3;

-- 第3题：订单量统计
CREATE TABLE test_sql.test3
(
    dt       string,
    order_id string,
    user_id  string,
    amount   DECIMAL(10, 2)
)
    ROW format delimited FIELDS TERMINATED BY '\t';
INSERT overwrite TABLE test_sql.test3
VALUES ('2017-01-01', '10029028', '1000003251', 33.57),
       ('2017-01-01', '10029029', '1000003251', 33.57),
       ('2017-01-01', '100290288', '1000003252', 33.57),
       ('2017-02-02', '10029088', '1000003251', 33.57),
       ('2017-02-02', '100290281', '1000003251', 33.57),
       ('2017-02-02', '100290282', '1000003253', 33.57),
       ('2017-11-02', '10290282', '100003253', 234),
       ('2018-11-02', '10290284', '100003243', 234);
--  (1)给出 2017年每个月的订单数、用户数、总成交金额。
select month(dt), count(order_id), count(user_id), sum(amount)
from test3
group by dt
having year(dt) = 2017;
--  (2)给出2017年11月的新客数(指在11月才有第一笔订单)
WITH temp3 AS (
  SELECT
    user_id,
    MIN(date_format(dt, 'yyyy-MM')) AS first_month
  FROM test3
  GROUP BY user_id
)
SELECT COUNT(*) AS new_customer_count
FROM temp3
WHERE first_month = '2017-11';

--  (3)统计每个月的新客户数
WITH temp3 AS (
  SELECT
    user_id,
    MIN(date_format(dt, 'yyyy-MM')) AS first_month
  FROM test3
  GROUP BY user_id
)
SELECT first_month,COUNT(*) AS new_customer_count
FROM temp3 group by first_month;


-- 第4题：大数据排序统计
CREATE TABLE test_sql.test4user
 (user_id string,name string,age int);
 CREATE TABLE test_sql.test4log
 (user_id string,url string);
 INSERT INTO TABLE test_sql.test4user VALUES('001','u1',10),
 ('002','u2',15),
 ('003','u3',15),
 ('004','u4',20),
 ('005','u5',25),
 ('006','u6',35),
 ('007','u7',40),
 ('008','u8',45),
 ('009','u9',50),
 ('0010','u10',65);
 INSERT INTO TABLE test_sql.test4log VALUES('001','url1'),
 ('002','url1'),
 ('003','url2'),
 ('004','url3'),
 ('005','url3'),
 ('006','url1'),
 ('007','url5'),
 ('008','url7'),
 ('009','url5'),
 ('0010','url1');


CREATE TABLE test5 (
  name  STRING,
  month STRING,
  amt   INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
INSERT INTO test5 VALUES
  ('张三', '01', 100),
  ('李四', '02', 120),
  ('王五', '03', 150),
  ('赵六', '04', 500),
  ('张三', '05', 400),
  ('李四', '06', 350),
  ('王五', '07', 180),
  ('赵六', '08', 400);

-- Step 1: 聚合每个人的总金额
WITH sum_amt AS (
  SELECT name, SUM(amt) AS total_amt
  FROM test5
  GROUP BY name
),

-- Step 2: 计算排名和占比
ranked AS (
  SELECT
    name,
    total_amt,
    ROW_NUMBER() OVER (ORDER BY total_amt DESC) AS rn
  FROM sum_amt
),

-- Step 3: 汇总总金额用于占比计算
total_sum AS (
  SELECT SUM(total_amt) AS all_amt FROM sum_amt
)

-- 最终查询：带上占比
SELECT
  r.name,
  r.total_amt,
  r.rn AS rank,
  concat(ROUND(r.total_amt / t.all_amt, 4)*100,'%') AS ratio
FROM ranked r
JOIN total_sum t
ORDER BY rank;




