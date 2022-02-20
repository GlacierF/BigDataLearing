package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 计算窗口内传感器读数的平均值
  @author : Altman
 */
public class TemperatureAverageDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<SensorReading> source = env.addSource(new SensorSource());
        // 设置水位线策略
        SingleOutputStreamOperator<SensorReading> eventTimeSource = source.assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );
        eventTimeSource
                // 按传感器id进行分区
                .keyBy(SensorReading::getId)
                // 定义窗口分配器为事件时间滚动窗口，大小为1秒
//                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .timeWindow(Time.seconds(1))
                .apply(new TemperatureAverage())
                .print();
        env.execute() ;
    }
}
