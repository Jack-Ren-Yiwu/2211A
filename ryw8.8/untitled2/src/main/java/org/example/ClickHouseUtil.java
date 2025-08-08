package org.example;


import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Time : 2022/11/15 13:09
 * @Author :tzh
 * @File : ClickHouseUtil
 * @Project : gmall_flink
 **/
public class ClickHouseUtil {
    public static <T> SinkFunction<T> getSink(String sql) {

        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            //获取所有的属性信息
                            Field[] declaredFields = t.getClass().getDeclaredFields();
                            //遍历字段
                            int offset = 0;
                            for (int i = 0; i < declaredFields.length; i++) {
                                //获取字段
                                Field fields = declaredFields[i];
                                //设置私有属性可访问
                                fields.setAccessible(true);
                                //获取字段上的注解
                                TransientSink transientSink = fields.getAnnotation(TransientSink.class);
                                if (transientSink != null) {
                                    //存在该注解
                                    offset++;//也可以将加注解的属性放在实体类的最后面，就可以省去很多麻烦
                                    continue;
                                }
                                //获取值
                                Object value = fields.get(t);

                                //给预编译SQL对象赋值
                                preparedStatement.setObject(i + 1 - offset, value);

                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }, new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build());
    }

    public static int createTable(String sql) throws Exception {
        Class.forName(GmallConfig.CLICKHOUSE_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.CLICKHOUSE_URL);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        return preparedStatement.executeUpdate();
    }

//    public static void main(String[] args) throws Exception {
//        int table = createTable("create table if not exists keyword_stats_2021 ( stt DateTime," +
//                "edt DateTime, keyword String , source String ," +
//                "ct UInt64 , ts UInt64" +
//                ")engine =ReplacingMergeTree( ts)" +
//                "partition by toYYYYMMDD(stt) order by ( stt,edt,keyword,source );");
//        System.err.println(table);
//    }

}
