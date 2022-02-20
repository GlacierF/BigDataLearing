package com.altman.connectors.kafka;

import com.altman.util.SensorReading;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * 实现自定义的partitioner分区器：通过传感器的id值进行分区，保证相同id的传感器读数在同一个kafka分区中
 * @author : Altman
 */
public class MyKafkaPartitioner extends FlinkKafkaPartitioner<SensorReading> {
    @Override
    public int partition(SensorReading record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        int hashCode = record.getId().hashCode();
        return partitions[hashCode % partitions.length];
    }
}
