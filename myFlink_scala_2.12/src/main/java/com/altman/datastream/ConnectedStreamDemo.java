package com.altman.datastream;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Created on 2021/11/23 8:37
 * Copyright (c) 2021/11/23,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class ConnectedStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Tuple3<String, Integer, Integer>> source1 = env.fromElements(Tuple3.of("a", 1, 2), Tuple3.of("b", 1, 2)
                );
        DataStreamSource<Tuple3<String, Integer, Integer>> source2 = env.fromElements(Tuple3.of("c", 3, 4), Tuple3.of("d", 3, 4));

        ConnectedStreams<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> connected = source1.connect(source2);

        connected.map(new CoMapFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>, Tuple3<String,Integer,Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map1(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple3.of(value.f0.toUpperCase(),value.f1,value.f2);
            }

            @Override
            public Tuple3<String, Integer, Integer> map2(Tuple3<String, Integer, Integer> value) throws Exception {
                return value;
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return value.f0;
            }
        }).print();

        env.execute();

    }
}
