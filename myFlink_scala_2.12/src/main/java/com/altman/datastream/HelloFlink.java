package com.altman.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Created on 2021/11/22 8:39
 * Copyright (c) 2021/11/22,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class HelloFlink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketSource = env.socketTextStream("192.168.202.130", 6666);
        socketSource.flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                for (String word : value.split(" ")) {
                    out.collect(Tuple2.of(word,1L));
                }
            }
        }).keyBy(value -> value.f0).print();
        env.execute();
    }
}
