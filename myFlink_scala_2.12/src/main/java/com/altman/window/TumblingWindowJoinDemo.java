package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created on 2021/12/4 12:10
 * Copyright (c) 2021/12/4,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class TumblingWindowJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<SensorReading> source1 = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

        SingleOutputStreamOperator<SensorReading> source2 = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

        source1.join(source2)
                .where(SensorReading::getId)
                .equalTo(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(200)))
                .apply(new JoinFunction<SensorReading, SensorReading, Tuple3<String,Long,Double>>() {
                    @Override
                    public Tuple3<String, Long, Double> join(SensorReading first, SensorReading second) throws Exception {
                        return Tuple3.of(first.getId(), first.getTimestamp(), first.getTemperature()+ second.getTemperature());
                    }
                }).print();
        env.execute();
    }
}
