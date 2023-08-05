package org.example.wh.sql;

public class CreateSql {
    public static String DataGenSource = "" +
            "CREATE TABLE source ( \n" +
            "    id INT, \n" +
            "    ts BIGINT, \n" +
            "    vc INT\n" +
            ") WITH ( \n" +
            "    'connector' = 'datagen', \n" +
            "    'rows-per-second'='1', \n" +
            "    'fields.id.kind'='random', \n" +
            "    'fields.id.min'='1', \n" +
            "    'fields.id.max'='10', \n" +
            "    'fields.ts.kind'='sequence', \n" +
            "    'fields.ts.start'='1', \n" +
            "    'fields.ts.end'='1000000', \n" +
            "    'fields.vc.kind'='random', \n" +
            "    'fields.vc.min'='1', \n" +
            "    'fields.vc.max'='100'\n" +
            ");\n";

    public static String DataGenGroupByKeySource = "" +
            " CREATE TABLE source1 (\n" +
            " dim STRING,\n" +
            " user_id BIGINT,\n" +
            " price BIGINT,\n" +
            " row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)),\n" +
            " WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
            " ) WITH (\n" +
            " 'connector' = 'datagen',\n" +
            " 'rows-per-second' = '1',\n" +
            " 'fields.dim.length' = '1',\n" +
            " 'fields.user_id.min' = '1',\n" +
            " 'fields.user_id.max' = '100000',\n" +
            " 'fields.price.min' = '1',\n" +
            " 'fields.price.max' = '100000'\n" +
            ");\n";


    public static String DataGenGroupByWindowSource="" +
            "CREATE TABLE ws (\n" +
            "  id INT,\n" +
            "  vc INT,\n" +
            "  pt AS PROCTIME(), --处理时间\n" +
            "  et AS cast(CURRENT_TIMESTAMP as timestamp(3)), --事件时间\n" +
            "  WATERMARK FOR et AS et - INTERVAL '5' SECOND   --watermark\n" +
            ") WITH (\n" +
            "  'connector' = 'datagen',\n" +
            "  'rows-per-second' = '1',\n" +
            "  'fields.id.min' = '1',\n" +
            "  'fields.id.max' = '1',\n" +
            "  'fields.vc.min' = '1',\n" +
            "  'fields.vc.max' = '100'\n" +
            ");\n";

}
