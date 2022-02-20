package com.altman.sourcAndSink;

import com.altman.util.SensorReading;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Properties;

/**
 * Created on 2021/12/25 13:39
 * Copyright (c) 2021/12/25,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class MyKafkaReader extends RichParallelSourceFunction<SensorReading> implements CheckpointedFunction {

    private volatile boolean isRunning = true;

    private transient KafkaConsumer<String,SensorReading> kafkaConsumer ;
    /**
     * 创建kafkaConsumer所需的配置，至少包含如下配置：
     * <ol>
     *     <li>bootstrap.servers</li>
     *     <li>topic</li>
     *     <li>value.deserializer</li>
     * </ol>
     */
    private final Properties properties ;

    private final Collection<TopicPartition> topicPartitions;

    public MyKafkaReader(Properties properties, Collection<TopicPartition> topicPartitions) {
        this.properties = properties;
        this.topicPartitions = topicPartitions;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        kafkaConsumer = new KafkaConsumer<String, SensorReading>(properties);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {



    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void run(SourceContext<SensorReading> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
