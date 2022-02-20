package com.altman.connectors.file;

import com.altman.util.SensorReading;
import com.altman.util.SensorSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

/**
 * 写出parquet文件
 * @author : Altman
 */
public class ParquetFileSinkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(20000L);
        // 获取数据源
        SingleOutputStreamOperator<SensorReading> source = env.addSource(new SensorSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forMonotonousTimestamps()
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
        // 编写parquet sink
        FileSink<SensorReading> sink = FileSink.forBulkFormat(
                //文件路径
                new Path("src/main/files"),
                // 内容格式
                ParquetAvroWriters.forReflectRecord(SensorReading.class))
                //一分钟一个桶
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd-HH--mm"))
                .withBucketCheckInterval(20000L)
                .build();
        source.sinkTo(sink);
        env.execute();
    }
}
