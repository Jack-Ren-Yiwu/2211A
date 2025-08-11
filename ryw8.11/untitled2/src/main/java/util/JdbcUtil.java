package util;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class JdbcUtil {
    public static <T> List<T> queryList(Connection connection,String querySql,Class<T> clz,boolean underScoreToCamel) throws Exception {

        //创建集合用于存放查询结果数据
        ArrayList<T> resultList = new ArrayList<>();

        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);

        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();

        //解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        //获取列数
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()){

            //创建泛型对象
            T t = clz.newInstance();

            for (int i = 1; i < columnCount+1; i++) {

                //获取列名
                String columnName = metaData.getColumnName(i);

                //判断是否需要转换为驼峰命名
                if (underScoreToCamel){
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }

                //获取列值
                Object value = resultSet.getObject(i);

                //给泛型对象赋值
                BeanUtils.setProperty(t,columnName,value);
            }
            //将该对象添加至集合
            resultList.add(t);
        }

        preparedStatement.close();
        resultSet.close();

        //返回结果集合
        return resultList;
    }


    //建表语句：create table if not exists db.tn(id varchar primary key,tm_name varchar,aaa varchar ,bb varchar) xxx;
    public static void createTable(String sinkTable,
                                   String sinkColumns,
                                   String sinkPk,
                                   String sinkExtend,
                                   Connection connection) {
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null) {
                sinkPk = "id";
            }
            if (sinkExtend == null) {
                sinkExtend = "";
            }
            StringBuffer createTableSQL = new StringBuffer("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            String[] fields = sinkColumns.split(",");
            for (int i = 0; i < fields.length; i++) {
                String field = fields[i];
                //判断是否为主键
                if (sinkPk.equals(field)) {
                    createTableSQL.append(field).append(" varchar primary key");
                } else {
                    createTableSQL.append(field).append(" varchar");
                }
                //判断是否为最后一个字段，如果不是，则添加 ","
                if (i < fields.length - 1) {
                    createTableSQL.append(" ,");
                }
            }
            createTableSQL.append(")").append(sinkExtend);
            //打印建表语句
            System.out.println(createTableSQL);
            //预编译SQL
            preparedStatement = connection.prepareStatement(createTableSQL.toString());
            //执行
            preparedStatement.execute();

        }catch(SQLException e){
            //e.printStackTrace();
            throw new RuntimeException("Phoenix表"+sinkTable+"建表失败!");
        }finally {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    //插入数据到PG
    public static <T> SinkFunction<T> getPgSink(String sql) {

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
                        .withDriverName(GmallConfig.PG_DRIVER)
                        .withUrl(GmallConfig.PG_URL)
                        .withUsername(GmallConfig.PG_USER)
                        .withPassword(GmallConfig.PG_PWD)
                        .build());
    }

    public static void main(String[] args) throws Exception {
        //System.out.println(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, "aa_bb"));
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> queryList = queryList(connection,
                "select * from \"htb_user_profile_tag\" where USER_ID ='952'",
                JSONObject.class,
                false);

        for (JSONObject jsonObject : queryList) {
            System.out.println(jsonObject);
        }

        connection.close();

    }
}
