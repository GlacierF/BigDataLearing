package com.altman.state;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * sensorReadingSource是一个普通的事件流，socketSource是一个规则流
 * 注意启动此类之前，需要先在虚拟机上开启 nc -lk 6666  用于后续传递数据到这边来，传递的数据格式形如 sensor_1 这个是传感器id编号，
 * 是sensor_加一个随机整数，具体看{@link SensorSource}里面的实现
 * @author : Altman
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取一条普通事件流
        KeyedStream<SensorReading, String> sensorReadingSource = env.addSource(new SensorSource()).keyBy(SensorReading::getId);
        // 获取一条规则流
        DataStreamSource<String> socketSource = env.socketTextStream("192.168.202.130", 6666);
        //将规则流广播为广播状态流
        MapStateDescriptor<String, String> rulesDesc = new MapStateDescriptor<>("rules", String.class, String.class);
        BroadcastStream<String> broadcast = socketSource.broadcast(rulesDesc);
        //连接两条流，注意是普通事件流调用connect方法，广播流作为参数传递到connect()中
        sensorReadingSource.connect(broadcast)
                .process(new MyKeyedBroadcastProcessFunction())
                .print();
        env.execute();
    }
}
