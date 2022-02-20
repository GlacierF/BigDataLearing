package com.altman.datastream;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created on 2021/11/23 7:40
 * Copyright (c) 2021/11/23,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class KeyedStreamSum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Integer>> source = env.fromElements(Tuple3.of("a", 1, 2),
                Tuple3.of("b", 1, 2), Tuple3.of("c", 1, 2), Tuple3.of("a", 1, 2),Tuple3.of("a",1,2));
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> reduce = source.keyBy(value -> value.f0).reduce(new ReduceFunction<Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1,
                                                           Tuple3<String, Integer, Integer> value2) throws Exception {
                return Tuple3.of(value1.f0, value1.f1 + value2.f1, value1.f2 + value2.f2);
            }
        });
        reduce.print();
        env.execute();
    }
}
