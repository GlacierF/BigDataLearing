package com.altman.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * Created on 2022/1/10 8:00 <br>
 * Copyright (c) 2022/1/10,一个很有个性的名字 所有 <br>
 * 测试FromChangelogStream的使用
 * @author : Altman
 */
public class FromChangelogStreamDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // ===== 示例 一  ====
        // 实现RowKind含有UPDATE_BEFORE,UPDATE_AFTER的retraction流
        DataStreamSource<Row> dataStream1 = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 12),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100)
        );
        //将changelog DataStream解释为Table
        Table inputTable1 = streamTableEnv.fromChangelogStream(dataStream1);
        // 向环境中注册这个Table
        streamTableEnv.createTemporaryView("InputTable1", inputTable1);
        //执行sql语句并打印结果
        streamTableEnv.executeSql("SELECT f0 AS name,SUM(f1) AS score FROM InputTable1 GROUP BY f0")
                .print();
        //打印结果如下：
        /*
        +----+--------------------------------+-------------+
        | op |                           name |       score |
        +----+--------------------------------+-------------+
        | +I |                            Bob |           5 |
        | +I |                          Alice |          12 |
        | -D |                          Alice |          12 |
        | +I |                          Alice |         100 |
        +----+--------------------------------+-------------+
         */

        // ==== 示例 二  ====
        // 实现RowKind只含有UPDATE_AFTER的upsert流
        DataStreamSource<Row> dataStream2 = env.fromElements(
                Row.ofKind(RowKind.INSERT, "Alice", 2),
                Row.ofKind(RowKind.INSERT, "Bob", 5),
                Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100)
        );
        Table inputTable2 = streamTableEnv.fromChangelogStream(dataStream2,
                Schema.newBuilder()
                        //指定`f0`为主键
                        .primaryKey("f0")
                        .build(),
                //指定DataStream是upsert流
                ChangelogMode.upsert()
        );
        //注册表格
        streamTableEnv.createTemporaryView("InputTable2",inputTable2);
        // 查询
        streamTableEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable2 GROUP BY f0").print();
        //打印结果如下；
        /*
        +----+--------------------------------+-------------+
        | op |                           name |       score |
        +----+--------------------------------+-------------+
        | +I |                            Bob |           5 |
        | +I |                          Alice |           2 |
        | -U |                          Alice |           2 |
        | +U |                          Alice |         100 |
        +----+--------------------------------+-------------+
         */
    }
}
