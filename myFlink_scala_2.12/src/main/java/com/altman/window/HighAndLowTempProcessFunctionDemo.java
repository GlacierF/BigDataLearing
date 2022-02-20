package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 计算每个传感器每个窗口读取到的最大最小温度
 * @author : Altman
 */
public class HighAndLowTempProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源，并配置水位线策略
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        source.keyBy(SensorReading::getId)
                 // 设置事件时间驱动的滚动窗口，大小为5秒
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new HighAndLowTempProcessFunction())
                .global();
        
//                .print();
        env.execute();
    }
}
