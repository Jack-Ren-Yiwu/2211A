package com.util;


import com.stream.common.utils.ConfigUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import java.util.Properties;

public class CdcSourceUtils {
    public static MySqlSource<String> getMySQLCdcSource(String username, String pass, String database, String table, StartupOptions model){

        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("database.connectionCharset", "UTF-8");
        debeziumProperties.setProperty("decimal.handling.mode","string");
        debeziumProperties.setProperty("time.precision.mode","connect");
        debeziumProperties.setProperty("snapshot.mode", "schema_only");
        debeziumProperties.setProperty("include.schema.changes", "false");
        debeziumProperties.setProperty("database.connectionTimeZone", "Asia/Shanghai");

        return MySqlSource.<String>builder()
                .hostname(ConfigUtils.getString("mysql.host"))
                .port(ConfigUtils.getInt("mysql.port"))
                .username(username)
                .password(pass)
                .databaseList(database)
                .tableList(table)
                .startupOptions(model)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();
    }
}
