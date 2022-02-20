package com.altman.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Instant;

/**
 * Created on 2022/1/9 17:33 <br>
 * Copyright (c) 2022/1/9,一个很有个性的名字 所有 <br>
 * toDataStream的代码示例
 * @author : Altman
 */
public class ToDataStreamDemo {
    /**
     * 实体类
     */
    public static class User{
        public String name ;
        public Integer score ;
        public Instant event_time ;
    }


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        streamTableEnv.executeSql( "CREATE TABLE GeneratedTable "
                + "("
                + "  name STRING,"
                + "  score INT,"
                + "  event_time TIMESTAMP_LTZ(3),"
                + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
                + ")"
                + "WITH ('connector'='datagen')");

        Table table = streamTableEnv.from("GeneratedTable");

        // ===== 示例一 ====
        // 使用默认的转换，将table中的记录转为DataStream中的Row对象，
        // 因为event_time是一个rowtime attribute（行时间属性），此属性也会作为row的一个字段
        // Table中的元数据和水位线策略会被自动继承过来
        DataStream<Row> dataStream = streamTableEnv.toDataStream(table);
        //dataStream.print();

        //通过反射的方式，获取User.class中的属性类型，并跟Table中记录的字段进行一一对应（可能会有隐式转换操作）
        // 因为event_time是一个rowtime attribute（行时间属性），此属性也会作为row的一个字段
        // Table中的元数据和水位线策略会被自动继承过来
        DataStream<User> userDataStream = streamTableEnv.toDataStream(table, User.class);

        //手动指定字段类型（感觉没必要）
        DataStream<Object> objectDataStream = streamTableEnv.toDataStream(table,
                DataTypes.STRUCTURED(User.class,
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("score", DataTypes.INT()),
                        DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))
                )
        );
        //objectDataStream.print() ;

        userDataStream.print();
        env.execute();
    }
}
