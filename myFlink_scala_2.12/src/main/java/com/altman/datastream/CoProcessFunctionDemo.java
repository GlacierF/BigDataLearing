package com.altman.datastream;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 运行结果是：一开始"sensor_2"和"sensor_7"两个分区都有数据，后面只剩"sensor_7"有数据，
 * 最后没有一个分区有数据了，因为转发的时间到了，{@link ReadingSwitcher}关闭了转发
 * @author : Altman
 */
public class CoProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //传感器事件流
        DataStreamSource<SensorReading> sensorSource = env.addSource(new SensorSource());
        DataStreamSource<Tuple2<String, Long>> switchSource = env.fromElements(Tuple2.of("sensor_2", 10 * 1000L), //表示允许转发10秒
                Tuple2.of("sensor_7", 60 * 1000L) // 表示允许转发60秒
        );
        ConnectedStreams<SensorReading, Tuple2<String, Long>> connect = sensorSource.connect(switchSource);
        SingleOutputStreamOperator<SensorReading> process = connect.keyBy(SensorReading::getId, value -> value.f0)
                .process(new ReadingSwitcher());
        process.print();
        env.execute();
    }
}
