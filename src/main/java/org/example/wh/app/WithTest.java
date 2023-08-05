package org.example.wh.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.wh.sql.CreateSql;

public class WithTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(CreateSql.DataGenSource);

        TableResult tableResult = tableEnv.executeSql("" +
                " with a as ( select id from source  ) " +
                " select sum(id) sm from a");


        tableResult.print();
    }
}
