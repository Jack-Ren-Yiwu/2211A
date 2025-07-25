package com.example.job;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
public class SqlServerCDCJob {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从集合中读取数据
        env.fromElements("hello", "flink", "world")
                // 转换操作，把字符串变成大写
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) {
                        return value.toUpperCase();
                    }
                })
                // 打印输出
                .print();

        // 启动作业
        env.execute("Simple Flink Job");
    }
}
