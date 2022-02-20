package com.altman.async;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * 异步请求MySQL数据库，注意你的MySQL数据库需要先插入几条数据，否则全部都是默认值
 * @author : Altman
 */
public class MysqlAsyncFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Long> idSource = env.addSource(new SensorSource())
                .map(new MapFunction<SensorReading, Long>() {
                    @Override
                    public Long map(SensorReading value) throws Exception {
                        // 拿到传感器读数的id
                        return Long.parseLong(value.getId().substring(value.getId().indexOf("sensor_") + 7));
                    }
                });
        // 异步函数处理
        AsyncDataStream.orderedWait(
                idSource,
                new MysqlAsyncFunction(),
                5,
                TimeUnit.SECONDS
        ).print();
        env.execute();
    }
}
