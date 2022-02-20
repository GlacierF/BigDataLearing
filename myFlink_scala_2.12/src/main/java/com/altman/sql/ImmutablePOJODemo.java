package com.altman.sql;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created on 2022/1/9 16:55 <br>
 * Copyright (c) 2022/1/9,一个很有个性的名字 所有 <br>
 * 不可变POJO的fromDataStream代码示例
 * @author : Altman
 */
public class ImmutablePOJODemo {
    /**
     * 使用final修饰属性，属于不可变POJO，目前DataStream不支持这样的数据类型
     * 因此所有的记录都会作为一个泛型传递
     */
    public static class User{
        public final String name ;
        public final Integer score ;

        public User(String name, Integer score) {
            this.name = name;
            this.score = score;
        }
    }

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<User> dataStream = env.fromElements(
                new User("Alice", 4),
                new User("Bob", 6),
                new User("Alice", 10)
        );
        // 因为字段的类型不能获取到，因此DataStream中的记录都被作为一个原子类型了
        // 从而导致Table只有一列数据，且字段名为`f0`
        streamTableEnv.fromDataStream(dataStream).printSchema();
        //打印结果如下：
        /*
        (
          `f0` RAW('com.altman.sql.ImmutablePOJODemo$User', '...')
        )
         */

        //====== 示例 二  =====
        // 通过反射的方式，自动获取字段类型
        streamTableEnv.fromDataStream(dataStream,
                Schema.newBuilder()
                .column("f0", DataTypes.of(User.class))
                .build()
                )
                //用as指定字段名
                .as("user")
                .printSchema();
        // 打印结果如下：
        /*
        (
        `user` *com.altman.sql.ImmutablePOJODemo$User<`name` STRING, `score` INT>*
        )
         */

        //===== 示例 三  ====
        // 收到指定字段类型
        streamTableEnv.fromDataStream(dataStream,
                Schema.newBuilder()
                .column("f0",DataTypes.STRUCTURED(
                        User.class,
                        DataTypes.FIELD("name",DataTypes.STRING()),
                        DataTypes.FIELD("score",DataTypes.INT())
                ))
                .build()
                )
                .as("user")
                .printSchema();
        //打印结果如下：跟示例 二 是一样的
        /*
        (
        `user` *com.altman.sql.ImmutablePOJODemo$User<`name` STRING, `score` INT>*
        )
         */
    }
}
