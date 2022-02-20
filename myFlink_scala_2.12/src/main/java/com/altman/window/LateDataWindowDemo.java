package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Created on 2021/12/4 13:40
 * Copyright (c) 2021/12/4,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class LateDataWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
        SingleOutputStreamOperator<Tuple3<String, Long, Integer>> process = source.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // 定义迟到事件的侧输出
                .sideOutputLateData(new OutputTag<SensorReading>("lateData") {})
                .process(new CountFunction());
        // 获取侧输出，并打印，注意在本示例中是没有迟到事件的，因此也就没有侧输出，所以这里不会打印东西
        // 这里只是提供了一个获取窗口侧输出的方式
        DataStream<SensorReading> lateStream = process.getSideOutput(new OutputTag<SensorReading>("lateData") {
        });
        lateStream.print();
        env.execute();
    }
}
