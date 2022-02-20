package com.altman.datastream;

import com.altman.datastream.FreezingMonitor;
import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * 侧输出代码示例
 * @author : Altman
 */
public class FreezingMonitorDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源
        DataStreamSource<SensorReading> reading = env.addSource(new SensorSource());
        // 处理数据，此处理函数里面实现了侧输出
        SingleOutputStreamOperator<SensorReading> monitoredReadings = reading.process(new FreezingMonitor());
        // 获取侧输出流
        DataStream<String> sideOutput = monitoredReadings.getSideOutput(new OutputTag<String>("freezing-alarms"){});
        //侧输出打印
        sideOutput.print();
        //主输出打印
        reading.print();
        env.execute();
    }
}
