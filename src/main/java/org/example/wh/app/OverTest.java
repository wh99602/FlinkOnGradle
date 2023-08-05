package org.example.wh.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.wh.sql.CreateSql;

public class OverTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(CreateSql.DataGenGroupByWindowSource);


        tableEnv
                .executeSql(" " +
                        " select " +
                        " id " +
                        " ,et" +
                        " ,count(1) over w ct" +
                        " ,sum(vc) over w sm" +
                        " from " +
                        " ws " +
                        " window w as (partition by id order by et " +
                        " rows between 5 preceding and current row) ").print();
    }
}
