package com.altman.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created on 2021/11/27 16:51
 * Copyright (c) 2021/11/27,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class WindowSumDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        long currentTimestamp = 1638000000999L;
        DataStreamSource<Tuple3<String, Long, Integer>> source = env.fromElements(Tuple3.of("a", currentTimestamp, 1),
                Tuple3.of("b", currentTimestamp + 1L, 1),
                Tuple3.of("c", currentTimestamp + 1001L, 1),
                Tuple3.of("a", currentTimestamp + 1000L, 1), Tuple3.of("b", currentTimestamp + 1L, 1));

        source.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((event,timestamp) -> event.f1))
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new WindowFunction<Tuple3<String, Long, Integer>, Tuple2<String,Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<Tuple3<String, Long, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
                        long start = window.getStart();
                        long end = window.getEnd();
                        Integer reduceValue = 0 ;
                        StringBuilder key = new StringBuilder();
                        for (Tuple3<String, Long, Integer> item : input) {
                            key.append(item.f0).append("-");
                            reduceValue += item.f2;
                        }
//                        out.collect(Tuple2.of(key.toString(),reduceValue));
                        System.out.println("====窗口开始时间是：" + start + "====窗口结束时间是：" + end + "==窗口需要发出的key是：" + key + "==窗口需要发出的值是：" + reduceValue);
                    }
                });
        env.execute();

    }
}
