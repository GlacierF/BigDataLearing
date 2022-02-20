package com.altman.datastream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created on 2021/11/25 8:43
 * Copyright (c) 2021/11/25,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class TypeInformationDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, String>> source = env.fromElements(Tuple2.of("a", "altman"), Tuple2.of("b", "bonnie"), Tuple2.of("c", "candy"));

        source.keyBy(1 );


        env.execute();
    }
}
