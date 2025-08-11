package util;

public class GmallConfig {
    //Phoenix 库名
    public static final String HBASE_SCHEMA = "FLINK_2208";

    //Phoenix 驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix 连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:cdh01,cdh02,cdh03:2181";

    //Clickhouse url连接地址
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://cdh01:8123";

    //Clickhouse 驱动
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";

    public static final String PG_DRIVER = "org.postgresql.Driver";
    public static final String PG_URL = "jdbc:postgresql://cdh01:5432/gmall_2208";
    public static final String PG_USER = "postgres";
    public static final String PG_PWD = "123456";

}
