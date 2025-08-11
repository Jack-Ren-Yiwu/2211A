package util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Connection;

@Slf4j
public class HBaseSinkFunction extends RichSinkFunction<JSONObject> {

    private transient Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化 HBase 连接
        connection = HBaseApiUtil.getHBaseConnection();
        if (connection == null || connection.isClosed()) {
            throw new RuntimeException("HBase connection is null or closed, please check configuration.");
        }
        log.info("✅ HBase connection established.");
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        if (value == null) {
            log.warn("⚠ Received null value, skipping...");
            return;
        }

        String tableName = value.getString("tableName"); // ✅ 与上游统一
        if (tableName == null) {
            log.warn("⚠ tableName is null, skipping...");
            return;
        }

        String rowKey = value.getString("id"); // after 已经是最外层了
        if (rowKey == null) {
            log.warn("⚠ No 'id' field found, skipping...");
            return;
        }

        // 建命名空间（只执行一次）
        HBaseApiUtil.createNameSpace(connection, GmallConfig.HBASE_SCHEMA);

        // 建表
        String fullTableName = "dim_" + tableName;
        HBaseApiUtil.createHBaseTable(connection, GmallConfig.HBASE_SCHEMA, fullTableName, "info");

        // 写数据
        HBaseApiUtil.putRow(connection, GmallConfig.HBASE_SCHEMA, fullTableName, rowKey, "info", value);
        log.info("✅ Wrote rowKey={} to HBase table={}", rowKey, fullTableName);
    }


    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null && !connection.isClosed()) {
            HBaseApiUtil.closeHBaseConn(connection);
            log.info("✅ HBase connection closed.");
        }
    }
}
