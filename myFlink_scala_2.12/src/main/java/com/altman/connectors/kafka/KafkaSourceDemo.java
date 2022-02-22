package com.altman.connectors.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;

/**
 * 设置读取kafkaSource，这里的wordCount主题只有两个分区，所以我们设置这两个分区在offset为5的时候推出
 * 你能看到一共打印了10条记录，每个kafka分区5条
 * @author : Altman
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        HashMap<TopicPartition, Long> topicPartitionOffset = new HashMap<TopicPartition, Long>();
        //设置读取0,1分区，且读到offset为5时结束，必须为所有的分区指定，否则会抛出异常
        // 异常是Error computing size for field 'key': Missing value for field 'key' which has no default value
        topicPartitionOffset.put(new TopicPartition("wordCount",0),5L);
        topicPartitionOffset.put(new TopicPartition("wordCount",1),5L);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("nn1.hadoop:9092,nn2.hadoop:9092,s1.hadoop:9092")
                // 设置读取的主题
                .setTopics("wordCount")
                // 从最早位置开始读
                .setStartingOffsets(OffsetsInitializer.earliest())
                //设置反序列化器，我们只对value感兴趣，所以使用了valueOnly
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
//                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 设置推出的位置
                .setUnbounded(OffsetsInitializer.offsets(topicPartitionOffset))
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"kafkaWordCount")
                .map(value -> Tuple2.of(value,1)).returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
                .keyBy(value -> value.f0)
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1))
                .print();
        env.execute();
    }
}
