package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created on 2021/11/27 15:52
 * Copyright (c) 2021/11/27,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class NoKeyedWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());

        source.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new ProcessAllWindowFunction<SensorReading, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<SensorReading> elements, Collector<String> out) throws Exception {
                        elements.forEach(element -> out.collect(element.getId().toUpperCase()));
                    }
                }).print();

        env.execute();
    }
}
