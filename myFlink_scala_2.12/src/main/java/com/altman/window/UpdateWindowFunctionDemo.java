package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 此示例实际上没有迟到事件，因此因此窗口虽然会保存到maxTimestamp + allowLateness这个时间，但是并不会触发二次运算
 * @author : Altman
 */
public class UpdateWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源并指定水位线策略
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

        source.keyBy(SensorReading::getId)
                // 定义一个事件时间滚动窗口，窗口大小为5秒钟
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //定义允许迟到1秒钟，凡是收到一个迟到时间就会触发一次运算
                .allowedLateness(Time.seconds(1))
                .process(new UpdateWindowFunction())
                .print();
        env.execute();
    }
}
