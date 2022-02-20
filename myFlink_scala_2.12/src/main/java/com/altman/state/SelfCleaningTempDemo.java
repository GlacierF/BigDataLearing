package com.altman.state;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 如果长时间没有看到结果，可以将阈值3.0改为2.0
 * @author : Altman
 */
public class SelfCleaningTempDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取数据源并配置水位线策略
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );

        source.keyBy(SensorReading::getId)
                // 处理函数阈值为3.0，长时间没有结果输出，可以改为2.0看看
                .process(new SelfCleaningTempAlertFunction(3.0))
                .print();
        env.execute();
    }
}
