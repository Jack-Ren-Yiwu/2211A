package org.example;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

@Slf4j
public class HBaseSinkFunction extends RichSinkFunction<JSONObject> {

    Connection connection = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = HBaseApiUtil.getHBaseConnection();
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        String tableName = "dim_" + value.getString("tableName");
        JSONObject after = value.getJSONObject("after");
        //创建命名空间
        HBaseApiUtil.createNameSpace(connection, GmallConfig.HBASE_SCHEMA);
        ////建表
        HBaseApiUtil.createHBaseTable(connection, GmallConfig.HBASE_SCHEMA, tableName, "info");
        //插入数据
        HBaseApiUtil.putRow(connection, GmallConfig.HBASE_SCHEMA, tableName, after.getString("id"), "info", after);
    }

    @Override
    public void close() throws Exception {
        super.close();
        HBaseApiUtil.closeHBaseConn(connection);
    }
}
