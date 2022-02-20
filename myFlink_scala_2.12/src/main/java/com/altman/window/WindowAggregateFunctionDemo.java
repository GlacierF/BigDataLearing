package com.altman.window;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 计算每个传感器每15秒内的平均温度
 * @author : Altman
 */
public class WindowAggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        // 获取数据源，并指定水位线策略
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
        source
                .keyBy(SensorReading::getId)
                // 定义事件时间驱动的滚动窗口，窗口大小为15秒
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                // 可以试着注释下面这样trigger代码，看看结果变化
                .trigger(new OneSecondIntervalTrigger())
                .aggregate(new AvgTempFunction())
                .print();
        env.execute();
    }
}
