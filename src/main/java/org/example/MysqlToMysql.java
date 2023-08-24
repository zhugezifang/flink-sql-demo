package org.example;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class MysqlToMysql {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String schema = "id BIGINT ,cnt BIGINT";
        String source_table = "test";
        String sink_table = "test_copy";
        String flink_source_table = "mysource";
        String flink_sink_table = "mysink";

        String mysql_sql = "CREATE TABLE %s (%s) WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://10.71.7.89:3306/temp?autoReconnect=true&useSSL=false',\n" +
                "   'table-name' = '%s',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456'\n" +
                " )";

        String source_ddl = String.format(mysql_sql, flink_source_table, schema, source_table);
        String sink_ddl = String.format(mysql_sql, flink_sink_table, schema, sink_table);

        tableEnvironment.executeSql(source_ddl);
        tableEnvironment.executeSql(sink_ddl);

        String insertsql = String.format("insert into %s select * from %s", flink_sink_table, flink_source_table);
        tableEnvironment.executeSql(insertsql).print();
    }
}
