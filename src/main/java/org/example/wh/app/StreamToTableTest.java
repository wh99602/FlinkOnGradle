package org.example.wh.app;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class StreamToTableTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketSource = env.socketTextStream("43.143.254.23", 8888);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("create table abb (" +
                " `id` string " +
                " )with(" +
                " 'connector' = 'whaaa' " +
//                " ,'home' = 'jdbc:mysql://43.143.254.23:3306/wh_test' " +
                " ,'url' = 'jdbc:mysql://43.143.254.23:3306/wh_test' " +
                " ,'table-name' = 'flink_test' " +
                " ,'username' = 'root' " +
                " ,'password' = '000000' " +
                " ,'writenull' = 'false' " +
                ")");
        Table table = tableEnv.fromDataStream(socketSource);

        tableEnv.executeSql("insert into abb select * from "+table);
//        " ,'home' = 'jdbc:mysql://43.143.254.23:3306/wh_test' " +

//        tableEnv.executeSql("select id from a").print();


    }
}
