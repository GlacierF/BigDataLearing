package com.altman.connectors.kafka;

import com.altman.util.SensorReading;
import com.altman.window.AvgTempFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;

/**
 * 实现读取kafka中的数据，这里水位线策略声明了分区空闲的时间，保证了部分分区在没有事件的情况下，水位线也能正常推进
 * @author : Altman
 */
public class SensorReadingFromKafka {
    public static void main(String[] args) throws Exception {
        KafkaSource<SensorReading> source = KafkaSource.<SensorReading>builder()
                .setBootstrapServers("nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
                .setTopics("SensorReading")
                .setValueOnlyDeserializer(new MyDeserializationSchema())
                .setStartingOffsets(OffsetsInitializer.earliest())
                // 设置只读取已经提交的事务数据，因为我们sink端是用了事务写kafka的，所以我们这里需要配置只读事务
                // 默认是读取所有数据，会导致没有提交成功的数据也被读到了
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置水位线策略
        WatermarkStrategy<SensorReading> watermarkStrategy = WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                //部分kafka分区的可能没有数据，所以要声明为空闲
                .withIdleness(Duration.ofSeconds(1));
        DataStreamSource<SensorReading> sensorSource = env.fromSource(source, watermarkStrategy, "sensor reading from kafka");
        sensorSource.keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .aggregate(new AvgTempFunction())
                .print();
        env.execute();
    }
}
