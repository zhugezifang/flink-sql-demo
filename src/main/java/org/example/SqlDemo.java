package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class SqlDemo {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //把kafka 中的topic映射成一个输入临时表
        tableEnv.executeSql(
                "create table sensor_source (id string,name string) with (" +
                        " 'connector' = 'kafka'," +
                        "  'topic' = 'test_info_test'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'properties.group.id' = 'testGroup'," +
                        "  'scan.startup.mode' = 'earliest-offset'," +
                        "  'format' = 'json')"
        );
        //把mysql 中的表映射成一个输出临时表

        String sql = "CREATE TABLE print_table (\n" +
                " id STRING,\n" +
                " name STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")";

        String mysql_sql = "CREATE TABLE mysql_sink (\n" +
                "                   id string,\n" +
                "                   name string\n" +
                " ) WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://ip:8081/kafka?serverTimezone=UTC',\n" +
                "   'table-name' = 'test_info',\n" +
                "   'username' = 'kafka',\n" +
                "   'password' = 'Bonc@123'\n" +
                " )";

        String kafka_sink_sql=
                "create table kafka_sink (id string,name string) with (" +
                        " 'connector' = 'kafka'," +
                        "  'topic' = 'test_info_2'," +
                        "  'properties.bootstrap.servers' = 'localhost:9092'," +
                        "  'format' = 'json')";

        tableEnv.executeSql(mysql_sql);
        //tableEnv.executeSql(kafka_sink_sql);
        //tableEnv.executeSql(sql);
        //插入数据的sql语句
        //tableEnv.executeSql("insert into print_table select * from sensor_source");

        tableEnv.executeSql("insert into mysql_sink select * from sensor_source");


        //tableEnv.executeSql("insert into kafka_sink select * from sensor_source");

    }
}
