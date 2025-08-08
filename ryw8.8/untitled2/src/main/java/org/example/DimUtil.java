package org.example;

import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * @Time : 2022/11/14 20:23
 * @Author :tzh
 * @File : DimUtil
 * @Project : gmall_flink
 **/
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        //查询Phoenix之前先查询Redis
      /*  Jedis jedis = RedisUtil.getJedis();
        //DIM:DIM_USER_INFO:143
        String redisKey = "DIM:" + tableName + ":"+ id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null){
            //重置过期时间
            jedis.expire(redisKey,24*60*60);
            //归还连接
            jedis.close();
            //返回结果
            return JSONObject.parseObject(dimInfoJsonStr);
        }*/
        /*
            Redis
            1.存什么数据？
                json字符串
            2.使用什么类型？
                String      Set     Hash
            3.RedisKey？
                String：tableName+id
                Set:  tableName
                hash: tableName   id
              不选Set的原因：
                1.查询不方便
                2.设置过期时间
             不选Hash原因：
                1.用户维度数据量大。
                2.设置过期时间
         */

        //拼接查询语句
        //select * from db.tn where id='18';
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id='" + id + "'";

        //查询Phoenix
        List<JSONObject> queryList = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfoJson = null;
        if (queryList.size() > 0) {
            dimInfoJson = queryList.get(0);
        }


        //在返回结果之前，将数据写入redis
        /*jedis.set(redisKey,dimInfoJson.toJSONString());
        jedis.expire(redisKey,24*60*60);
        jedis.close();
*/
        //返回结果
        return dimInfoJson;
    }

    //DimSinkFunction 负责往HBase写
  /*  public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }*/

    public static void main(String[] args) throws Exception {
        /*Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);


        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,"DIM_USER_INFO","999"));
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection,"DIM_USER_INFO","998"));
        long end2 = System.currentTimeMillis();
        //对于HBase而言是有客户端缓存的，所以第二次查的速度会比第一次块
        System.out.println(end - start);
        System.out.println(end2 - end);*/

        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "508"));
        long end = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "508"));
        long end2 = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_USER_INFO", "508"));
        long end3 = System.currentTimeMillis();
        System.out.println(end - start);
        System.out.println(end2 - end);
        System.out.println(end3 - end2);

       /* Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        System.out.println(getDimInfo(connection,"DIM_BASE_TRADEMARK","15"));*/

        connection.close();
    }
}
