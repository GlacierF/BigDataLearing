package com.altman.sql;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

/**
 * Created on 2022/1/9 16:07 <br>
 * Copyright (c) 2022/1/9,一个很有个性的名字 所有 <br>
 * 测试fromDataStream()方法的使用，请留意注释内容
 * @author : Altman
 */
public class FromDataStreamDemo {
    /**
     * POJO 实例类，POJO的前提可以自己查
     */
    public static class User {
        // pojo的属性要么是public的，要么提供了public的getter和setter
        public String name;
        public Integer score;
        public Instant event_time;

        //pojo需要的空构造器
        public User() {
        }

        public User(String name, Integer score, Instant event_time) {
            this.name = name;
            this.score = score;
            this.event_time = event_time;
        }
    }

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        //创建DataStream
        DataStreamSource<User> dataStream = env.fromElements(
                new User("Alice", 4, Instant.ofEpochMilli(1000)),
                new User("Bob", 6, Instant.ofEpochMilli(1001)),
                new User("Alice", 10, Instant.ofEpochMilli(1002))
        );
        //为示例四和五做准备，指定stream record的时间戳和水位线策略
        dataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<User>forMonotonousTimestamps()
                .withTimestampAssigner((event,timestamp) -> event.event_time.toEpochMilli())
        );
        //==== 示例一 =====
        // 自动根据POJO，取得字段名及其DataType，字段名就是属性名
        Table table = streamTableEnv.fromDataStream(dataStream);
        table.printSchema();
        //打印出的表结构如下：
//        (
//            `name`STRING,
//            `score`INT,
//            `event_time`TIMESTAMP_LTZ(9)
//        )

        // ====== 示例 二 =====
        // 自动根据POJO获取字段名及其类型，但是额外增加一列计算列（处理时间属性列表，使用内置函数PROCTIME()）
        Table table2 = streamTableEnv.fromDataStream(dataStream,
                Schema.newBuilder()
                        .columnByExpression("proc_time", "PROCTIME()")
                        .build()
        );
        table2.printSchema();
        //打印结果如下：
        /*
        (
             `name` STRING,
             `score` INT,
             `event_time` TIMESTAMP_LTZ(9),
             `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
        )
         */
        //===== 示例 三 =====
        // 自动从POJO中获取字段名及其类型，但是增加一个计算列（创建一个rowtime attribute），并添加水位线策略
        streamTableEnv.fromDataStream(dataStream,
                Schema.newBuilder()
                .columnByExpression("rowtime1","CAST(event_time AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime1","rowtime1 - INTERVAL '10' SECOND")
                .build()
        ).printSchema();
        //打印结果如下：由此可以看出，只要某列字段为WATERMARK提供信息了，那么这个字段就会被标记为rowtime，而不管字段名
        /*
        (
            `name` STRING,
            `score` INT,
            `event_time` TIMESTAMP_LTZ(9),
            `rowtime1` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
             WATERMARK FOR `rowtime1`: TIMESTAMP_LTZ(3) AS rowtime1 - INTERVAL '10' SECOND
          )
         */

        //====== 示例四  =====
        // 自动从POJO中火气字段名及其类型，同时获取stream record的timestamp，并使用SOURCE_WATERMARK()内置函数
        // 来继承DataStream中已有的水位线策略。此方式需要DataStream已经指定了时间戳(timestamp)和水位线策略
        streamTableEnv.fromDataStream(dataStream,
                Schema.newBuilder()
                        //注意这里的方法名是columnByMetadata()
                .columnByMetadata("rowtime","TIMESTAMP_LTZ(3)")
                .watermark("rowtime","SOURCE_WATERMARK()")
                .build()
        ).printSchema();
        //打印结果如下：
        /*
        (
        `name` STRING,
         `score` INT,
         `event_time` TIMESTAMP_LTZ(9),
         `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
          WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
        )
         */
        //======= 示例 五  =====
        // 手动定义字段的类型（注意字段名还是要跟POJO的一致），因为水位线策略的时间类型必须是TIMESTAMP(3)的，
        // 因此我们这里修改了精确度，原本是TIMESTAMP_LTZ(9)现在是3
        streamTableEnv.fromDataStream(dataStream,
                Schema.newBuilder()
                .column("event_time","TIMESTAMP_LTZ(3)")
                .column("name","STRING")
                .column("score","INT")
                .watermark("event_time","SOURCE_WATERMARK()")
                .build()
                ).printSchema();
        //打印结果如下：这里之所以没有打印水位线策略，是因为我们在schema中重新构建了字段顺序
        // 当且仅当schema中的字段顺序跟POJO的一致的话，那么就会打印水位线策略
        /*
        (
            `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
            `name` STRING,
            `score` INT
        )
         */
    }
}
