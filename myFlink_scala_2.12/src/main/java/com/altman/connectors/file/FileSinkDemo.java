package com.altman.connectors.file;


import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

/**
 * Created on 2021/12/12 17:59
 * Copyright (c) 2021/12/12,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // stream模式下，必须开启checkpoint，否则所有的文件永远是in-progress或pending状态
        env.enableCheckpointing(20000L);
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource());

        FileSink<SensorReading> sink = FileSink
                .forBulkFormat(new Path("src/main/files"), AvroWriters.forReflectRecord(SensorReading.class))
                // 每分钟一个桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd-HH--mm"))
                .withBucketCheckInterval(60 * 1000L)
                .build();

        source.sinkTo(sink);

        env.execute();

    }
}
