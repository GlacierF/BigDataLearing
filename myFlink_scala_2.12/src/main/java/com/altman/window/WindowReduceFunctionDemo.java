package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Created on 2021/11/28 15:13
 * Copyright (c) 2021/11/28,一个很有个性的名字 所有
 * 计算每15的的最低温度
 * @author : Altman
 */
public class WindowReduceFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源，并指定水位线策略
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource());
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<SensorReading>forMonotonousTimestamps()
//                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        source
                .map(value -> Tuple2.of(value.getId(),value.getTemperature()))
                // 因为map里面使用了lambda，所以这里需要使用returns指定map返回值的类型，否则会报错，当然也可以使用匿名类的形式
                .returns(new TypeHint<Tuple2<String, Double>>() {})
                .keyBy(value -> value.f0)
                // 定义事件时间滚动窗口，窗口大小为15秒
                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .reduce(new ReduceFunction<Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> reduce(Tuple2<String, Double> value1, Tuple2<String, Double> value2) throws Exception {
                        return Tuple2.of(value1.f0,Math.min(value1.f1, value2.f1));
                    }
                })
                .print();
        env.execute();
    }
}
