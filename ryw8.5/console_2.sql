CREATE TABLE test6
(
    userid      varchar(255),
    money       decimal(10, 2),
    paymenttime varchar(255),
    orderid     varchar(255)
);
INSERT INTO test6
VALUES ('001', 100, '2017-10-01', '123'),
       ('001', 200, '2017-10-02', '124'),
       ('002', 500, '2017-10-01', '125'),
       ('001', 100, '2017-11-01', '126');
--写出所有用户中在今年10月份第一次购买商品的金额
with temp as (select *, rank() over (partition by userid order by paymenttime ) as py from test6)
select *
from temp
where month(paymenttime) = 10
  and py = 1;


CREATE TABLE user_logs
(
    log_time     VARCHAR(255),
    api_endpoint VARCHAR(255),
    ip_address   VARCHAR(50)
);
INSERT INTO user_logs
VALUES ('2016-11-09 14:22:05', '/api/user/login', '110.23.5.33'),
       ('2016-11-09 14:23:10', '/api/user/detail', '57.3.2.16'),
       ('2016-11-09 15:59:40', '/api/user/login', '200.6.5.166'),
       ('2016-11-09 16:01:22', '/api/user/logout', '192.168.1.10'),
       ('2016-11-09 16:15:35', '/api/user/register', '172.16.0.5'),
       ('2016-11-09 17:22:40', '/api/user/login', '66.249.75.34'),
       ('2016-11-09 18:10:10', '/api/user/detail', '203.0.113.12'),
       ('2016-11-09 18:15:45', '/api/user/update', '198.51.100.45'),
       ('2016-11-09 19:01:55', '/api/user/logout', '10.0.0.3'),
       ('2016-11-09 19:22:20', '/api/user/detail', '203.0.113.99'),
       ('2016-11-09 20:10:30', '/api/user/login', '123.45.67.89'),
       ('2016-11-09 20:59:59', '/api/user/update', '8.8.8.8'),
       ('2016-11-09 21:35:12', '/api/user/register', '114.114.114.114');

--求11月9号下午14点（15-19点），访问/api/user/login接口的top10的ip地址
select ip_address, count(*) as at, api_endpoint
from user_logs
where date_format(log_time, 'yyyy-MM-dd HH') >= '2016-11-09 15'
  and date_format(log_time, 'yyyy-MM-dd HH') <= '2016-11-09 19'
group by ip_address, api_endpoint
order by at desc
limit 10;


create database if not exists sql_mian;
use sql_mian;
create table action
(
    userId     varchar(255),
    visitDate  varchar(255),
    visitCount int
);
insert into action
values ('u01', '2017/1/21', 5),
       ('u02', '2017/1/23', 6),
       ('u03', '2017/1/22', 8),
       ('u04', '2017/1/20', 3),
       ('u01', '2017/1/23', 6),
       ('u01', '2017/2/21', 8),
       ('u02', '2017/1/23', 6),
       ('u01', '2017/2/22', 4);

--要求使用SQL统计出每个用户每月的累积访问次数
select userId
     , date_format(replace(visitDate, '/', '-'), '%Y-%M')
     , sum(visitCount)
     , sum(sum(visitCount)) over (partition by userId ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
from action
group by userId, date_format(replace(visitDate, '/', '-'), '%Y-%M');


create table visit
(
    user_id varchar(255),
    shop    varchar(255)
);
insert into visit (user_id, shop)
values ('u2', 'b'),
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
--每个店铺的UV（访客数）
select shop, count(DISTINCT user_id)
from visit
group by shop;
--每个店铺访问次数top3的访客信息。输出店铺名称、访客id、访问次数
with temp as (select shop, user_id, rank() over (partition by shop order by count(user_id) desc) as ar
              from visit
              group by shop, user_id)
select *
from temp
where ar <= 3;


CREATE TABLE STG_ORDER
(
    `Date`     DATE,
    `Order_id` BIGINT,
    `User_id`  BIGINT,
    `Amount`   DECIMAL(10, 2)
);
INSERT INTO STG_ORDER (`Date`, Order_id, User_id, Amount)
VALUES ('2016-12-25', 10028001, 1000003201, 50.00),
       ('2017-01-01', 10029001, 1000003202, 88.88),
       ('2017-01-03', 10029002, 1000003203, 23.45),
       ('2017-01-15', 10029003, 1000003204, 99.99),
       ('2017-02-01', 10029004, 1000003202, 12.34),
       ('2017-02-05', 10029005, 1000003205, 77.77),
       ('2017-03-10', 10029006, 1000003201, 150.00),
       ('2017-03-20', 10029007, 1000003206, 65.00),
       ('2018-01-10', 10029008, 1000003207, 200.00),
       ('2018-01-22', 10029009, 1000003208, 300.00),
       ('2018-02-14', 10029010, 1000003209, 49.99),
       ('2018-02-28', 10029011, 1000003210, 88.00),
       ('2018-03-01', 10029012, 1000003202, 129.90),
       ('2019-01-01', 10029013, 1000003211, 66.66),
       ('2019-01-15', 10029014, 1000003212, 77.77),
       ('2019-01-20', 10029015, 1000003213, 33.33),
       ('2019-02-01', 10029016, 1000003214, 21.21),
       ('2019-03-05', 10029017, 1000003215, 55.55),
       ('2020-01-01', 10029018, 1000003201, 120.00),
       ('2020-01-15', 10029019, 1000003216, 199.99),
       ('2020-02-20', 10029020, 1000003217, 88.88),
       ('2020-03-10', 10029021, 1000003218, 34.50),
       ('2021-01-01', 10029022, 1000003219, 45.00),
       ('2021-01-01', 10029023, 1000003220, 60.00),
       ('2021-01-02', 10029024, 1000003221, 90.00),
       ('2021-02-15', 10029025, 1000003222, 12.12),
       ('2021-03-03', 10029026, 1000003223, 180.80),
       ('2021-03-03', 10029027, 1000003224, 300.30),
       ('2022-01-01', 10029028, 1000003225, 55.55),
       ('2022-02-02', 10029029, 1000003226, 66.66),
       ('2022-03-15', 10029030, 1000003227, 77.77);

--给出 2017年每个月的订单数、用户数、总成交金额。
select date_format(Date, '%Y-%M'), count(Order_id), count(DISTINCT User_id), sum(Amount)
from STG_ORDER
where year(Date) = 2017
group by date_format(Date, '%Y-%M');


CREATE TABLE user_visit
(
    visit_date DATE,
    user_id    VARCHAR(50),
    age        INT
);
INSERT INTO user_visit (visit_date, user_id, age)
VALUES ('2019-02-11', 'test_1', 23),
       ('2019-02-11', 'test_2', 19),
       ('2019-02-11', 'test_3', 39),
       ('2019-02-11', 'test_1', 23),
       ('2019-02-11', 'test_3', 39),
       ('2019-02-11', 'test_1', 23),
       ('2019-02-12', 'test_2', 19),
       ('2019-02-13', 'test_1', 23),
       ('2019-02-15', 'test_2', 19),
       ('2019-02-16', 'test_2', 19);
--所有用户和活跃用户的总数及平均年龄。（活跃用户指连续两天都有访问记录的用户）
with temp as (select user_id, age from user_visit group by user_id, age),
     user_with_next_day as (select visit_date, user_id, age, DATE_ADD(visit_date, INTERVAL 1 DAY) as next_day from user_visit),
     active_users as (select DISTINCT u.user_id, u.age
                      from user_with_next_day u
                               join user_visit v on u.user_id = v.user_id and u.next_day = v.visit_date)
select count(DISTINCT t.user_id), count(DISTINCT au.user_id), ROUND(avg(DISTINCT t.age), 2), ROUND(avg(DISTINCT au.age), 2)
from temp t
       left join  active_users au on t.user_id = au.user_id;









