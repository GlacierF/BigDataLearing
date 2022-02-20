package com.altman.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * Created on 2022/1/9 14:27 <br>
 * Copyright (c) 2022/1/9,一个很有个性的名字 所有 <br>
 * 本示例是比较完备的DataStream与Table的转换示例了，为Row中的每个值都指定了字段名方便后续select查询而不思使用f0这样的默认字段名了
 * @author : Altman
 */
public class ChangelogUnification {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //你还可以设置为stream模式，来检查结果是否是统一的
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Row> userStream = env.fromElements(
                Row.of(LocalDateTime.parse("2021-08-21T13:00:00"), 1, "Alice"),
                Row.of(LocalDateTime.parse("2021-08-21T13:05:00"), 2, "Bob"),
                Row.of(LocalDateTime.parse("2021-08-21T13:10:00"), 2, "Bob")
        ).returns(Types.ROW_NAMED(
                new String[]{"ts", "uid", "name"},
                Types.LOCAL_DATE_TIME, Types.INT, Types.STRING
        ));

        SingleOutputStreamOperator<Row> orderStream = env.fromElements(
                Row.of(LocalDateTime.parse("2021-08-21T13:02:00"), 1, 122),
                Row.of(LocalDateTime.parse("2021-08-21T13:07:00"), 2, 239),
                Row.of(LocalDateTime.parse("2021-08-21T13:11:00"), 2, 999)
        ).returns(
                Types.ROW_NAMED(
                        new String[]{"ts", "uid", "amount"},
                        Types.LOCAL_DATE_TIME, Types.INT, Types.INT
                )
        );
        //将DataStream转为Table，并指定表结构
        streamTableEnv.createTemporaryView("UserTable", userStream,
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("uid", DataTypes.INT())
                        .column("name", DataTypes.STRING())
                        .watermark("ts", "ts - interval '1' second")
                        .build()
        );
        streamTableEnv.createTemporaryView("OrderTable", orderStream,
                Schema.newBuilder()
                        .column("ts", DataTypes.TIMESTAMP(3))
                        .column("uid", DataTypes.INT())
                        .column("amount", DataTypes.INT())
                        .watermark("ts", "ts - interval '1' second")
                        .build()
        );
        Table joinedTable = streamTableEnv.sqlQuery("SELECT u.name,o.amount \n" +
                "FROM UserTable u,OrderTable o \n" +
                "WHERE u.uid = o.uid AND o.ts BETWEEN u.ts AND u.ts + INTERVAL '5' MINUTES");

        streamTableEnv.toDataStream(joinedTable).print();
        env.execute() ;
        //打印结果如下：Stream模式和Batch模式结果是一样的，说明在基于时间时间和水位线的算子处理后，实现了changelog stream的统一
//        16> +I[Bob, 239]
//        16> +I[Bob, 999]
//        7> +I[Alice, 122]
    }
}
