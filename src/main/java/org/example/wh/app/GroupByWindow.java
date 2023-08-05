package org.example.wh.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.wh.sql.CreateSql;

public class GroupByWindow {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(CreateSql.DataGenGroupByWindowSource);

        //滚动窗口
//        tableEnv
//                .executeSql("" +
//                        " select id" +
//                        " ,TUMBLE_START(et,interval '5' second) win_start " +
//                        " ,TUMBLE_END(et,interval '5' second) win_end " +
//                        " ,TUMBLE_ROWTIME(et,interval '5' second) win_row " +
////                        " ,TUMBLE_PROCTIME(et,interval '5' second) win_proc " +
//                        " ,sum(vc) sm " +
//                        " from ws group by id,TUMBLE(et,interval '5' second)").print();


        //滑动窗口
//        tableEnv
//                .executeSql(" " +
//                        " select " +
//                        " id " +
//                        " ,HOP_START(et,interval '2' second,interval '5' second) win_start " +
//                        " ,HOP_END(et,interval '2' second,interval '5' second) win_start " +
//                        " ,sum(vc) sm " +
//                        " from" +
//                        " ws" +
//                        " group by id,HOP(et,interval '2' second,interval '5' second)").print();



        //TVF
        tableEnv
                .executeSql(" " +
                        " select id,window_start,window_end,sum(vc) sm" +
                        " from" +
                        " table( CUMULATE(table ws,DESCRIPTOR(et),interval '2' second,interval '6' second) )" +
                        " group by id,window_start,window_end").print();


    }
}
