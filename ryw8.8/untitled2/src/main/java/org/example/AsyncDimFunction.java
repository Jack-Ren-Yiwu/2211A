package org.example;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.client.AsyncConnection;

import java.util.Collections;



public abstract class AsyncDimFunction<T> extends RichAsyncFunction<T, T> implements DimFunction<T> {

    private AsyncConnection hBaseAsyncConn;

    @Override
    public void open(Configuration parameters) throws Exception {
        hBaseAsyncConn = HBaseApiUtil.getHBaseAsyncConnection();
    }

    @Override
    public void close() throws Exception {
        HBaseApiUtil.closeAsyncHbaseConnection(hBaseAsyncConn);
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        JSONObject jsonObject = HBaseApiUtil.readDimAsync(hBaseAsyncConn,
                GmallConfig.HBASE_SCHEMA, //namespace
                getTableName(),  // 表名
                getRowKey(input));// rowkey 主键

        addDims(input, jsonObject);
        resultFuture.complete(Collections.singleton(input));
    }

}
