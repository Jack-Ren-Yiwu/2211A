package com.app.ods;

import java.util.HashMap;

public class test {
    public static int[] test1(int[] num,int  target){
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < num.length; i++) {
            if (map.containsKey(target - num[i])) {
                return new int[]{map.get(target - num[i]), i};
            }
            map.put(num[i], i);
        }
        return new int[]{-1, -1};
    }
    public static void main(String[] args) {
        int[] ints = test1(new int[]{2, 7, 11, 15}, 9);
        System.out.println("结果1: [" + ints[0] + ", " + ints[1] + "]");

        int[] ints1 = test1(new int[]{3, 2, 4}, 6);
        System.out.println("结果2: [" + ints1[0] + ", " + ints1[1] + "]");

        int[] ints2 = test1(new int[]{3, 3}, 6);
        System.out.println("结果3: [" + ints2[0] + ", " + ints2[1] + "]");

    }
}
//求每一天登录的新老用户数
//
//题目描述
//用户登录表 fact_log
//user_id
//device_id
//login_date
//如果用户第一天登录算新用户，求每一天登录的新老用户数

//WITH temp AS (
//    SELECT
//        user_id,
//        MIN(login_date) AS first_date
//    FROM fact_log
//    GROUP BY user_id
//)
//SELECT
//    f.login_date,
//    SUM(CASE WHEN f.login_date = fl.first_date THEN 1 ELSE 0 END) AS new_user_cnt,
//    SUM(CASE WHEN f.login_date > fl.first_date THEN 1 ELSE 0 END) AS old_user_cnt
//FROM fact_log f
//JOIN temp fl
//    ON f.user_id = fl.user_id
//GROUP BY f.login_date
//ORDER BY f.login_date;