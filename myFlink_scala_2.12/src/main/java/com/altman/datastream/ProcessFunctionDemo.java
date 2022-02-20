package com.altman.datastream;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 此程序用于监控每个传感器 每秒内的温度变化，如果一个传感器读取的温度在一秒内一直是单调递增的，那么就会打印出一条警告
 * @author : Altman
 */
public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        // 获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStreamSource<SensorReading> reading = env.addSource(new SensorSource());
        // 按传感器id分区，并使用TempIncreaseAlertFunction函数处理事件
        reading.keyBy(SensorReading::getId).process(new TempIncreaseAlertFunction()).print();
        env.execute();
    }
}
