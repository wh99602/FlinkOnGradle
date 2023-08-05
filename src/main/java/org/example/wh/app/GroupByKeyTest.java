package org.example.wh.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.wh.sql.CreateSql;

public class GroupByKeyTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        tableEnv.executeSql(CreateSql.DataGenGroupByKeySource);
//
//        tableEnv.executeSql("select dim,max(price) mx from source1 where dim='0' group by dim").print();

        //多维分析
        tableEnv.executeSql(" SELECT supplier_id, rating, COUNT(*) AS total\n" +
                " FROM (VALUES\n" +
                "    ('supplier1', 'product1', 4),\n" +
                "    ('supplier1', 'product2', 3),\n" +
                "    ('supplier2', 'product3', 3),\n" +
                "    ('supplier2', 'product4', 4))\n" +
                " AS Products(supplier_id, product_id, rating)\n" +
                " GROUP BY GROUPING SETS ((supplier_id, rating), (supplier_id), ())").print();
    }
}
