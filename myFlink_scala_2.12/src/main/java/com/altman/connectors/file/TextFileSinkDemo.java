package com.altman.connectors.file;

import com.altman.connectors.file.encoder.SensorReadingEncoder;
import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

/**
 * 使用自定义的encoder，写出text类型的文件
 * @author : Altman
 */
public class TextFileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20000);

        FileSink<SensorReading> sink = FileSink.forRowFormat(new Path("src/main/files_text"), new SensorReadingEncoder())
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd-HH--mm"))
                .withBucketCheckInterval(20000)
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .build();

        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource()).assignTimestampsAndWatermarks(
                WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        source.sinkTo(sink);
        env.execute();
    }
}
