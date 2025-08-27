package com.app.func;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.HbaseUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;


public class MapUpdateHbaseDimTableFunc extends RichMapFunction<JSONObject,JSONObject> {

    private Connection connection;
    private final String hbaseNameSpace;
    private final String zkHostList;
    private HbaseUtils hbaseUtils;

    public MapUpdateHbaseDimTableFunc(String cdhZookeeperServer, String cdhHbaseNameSpace) {
        this.zkHostList = cdhZookeeperServer;
        this.hbaseNameSpace = cdhHbaseNameSpace;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        hbaseUtils = new HbaseUtils(zkHostList);
        connection = hbaseUtils.getConnection();
    }

    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        String op = jsonObject.getString("op");

        JSONObject after = jsonObject.getJSONObject("after");
        if (after == null) {
            System.err.println("❌ after 字段为 null，原始 jsonObject = " + jsonObject);
            return jsonObject;
        }

        String tableName = after.getString("sink_table");
        if (tableName == null || tableName.trim().isEmpty()) {
            System.err.println("❌ sink_table 字段为空，after = " + after);
            return jsonObject;  // 避免 NPE
        }

        String fullTableName = hbaseNameSpace + ":" + tableName;

        if ("d".equals(op)) {
            hbaseUtils.deleteTable(fullTableName);
            System.out.println("🗑 删除表：" + fullTableName);
        } else if ("r".equals(op) || "c".equals(op)) {
            if (!hbaseUtils.tableIsExists(fullTableName)) {
                hbaseUtils.createTable(hbaseNameSpace, tableName);
                System.out.println("✅ 创建表：" + fullTableName);
            }
        } else {
            hbaseUtils.deleteTable(fullTableName);
            hbaseUtils.createTable(hbaseNameSpace, tableName);
            System.out.println("♻️ 重建表：" + fullTableName);
        }

        return jsonObject;
    }


    @Override
    public void close() throws Exception {
        super.close();
        connection.close();
    }
}
