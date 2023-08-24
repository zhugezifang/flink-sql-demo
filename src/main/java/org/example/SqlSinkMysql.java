package org.example;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class SqlSinkMysql {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // Create a table with two columns: id and name using datagen connector
        tEnv.executeSql("CREATE TABLE myTable (id INT, cnt BIGINT) WITH ('connector'='datagen','rows-per-second'='5','fields.id.min'='1','fields.id.max'='500')");

        tEnv.executeSql("CREATE TABLE mysqlSinkTable (id INT, cnt BIGINT,primary key (id) not enforced) " +
                "WITH('connector'='jdbc', 'url'='jdbc:mysql://ip:3306/temp?autoReconnect=true&useSSL=false', " +
                "'table-name'='test', 'username'='root', 'password'='123456', " +
                "'driver'='com.mysql.jdbc.Driver')");

        String sql1 = "CREATE view tmp_view as select * from myTable";

        String sql = "CREATE TABLE print_table (\n" +
                " id INT,\n" +
                " cnt BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")";

        // Insert the generated data into the MySQL sink table
        tEnv.executeSql(sql);
        tEnv.executeSql(sql1);

        tEnv.executeSql("insert INTO mysqlSinkTable SELECT id,cnt FROM tmp_view");
        tEnv.executeSql("insert into print_table select id,cnt from tmp_view");

        // Execute the job
        //env.execute();
    }
}


