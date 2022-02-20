package com.altman.datastream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Created on 2021/11/23 8:17
 * Copyright (c) 2021/11/23,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class DataStreamUnion {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<Integer> source1 = env.fromElements(1, 2, 3);
        DataStreamSource<Integer> source2 = env.fromElements(4, 5, 6);
        DataStreamSource<Integer> source3 = env.fromElements(7, 8, 9);
        DataStream<Integer> union = source1.union(source2, source3);
        union.print();
        env.execute();
    }
}
