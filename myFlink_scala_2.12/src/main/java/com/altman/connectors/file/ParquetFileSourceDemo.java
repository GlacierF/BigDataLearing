package com.altman.connectors.file;

import com.altman.util.SensorReading;
import com.altman.window.AvgTempFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;

import java.time.Duration;

/**
 * 读取parquet文件，注意{@link LogicalType}是org.apache.flink.table.types.logical.*这个包
 * @author : Altman
 */
public class ParquetFileSourceDemo {
    public static void main(String[] args) throws Exception {
        // 配置parquet文件行字段类型
        final LogicalType[] fieldTypes =
                new LogicalType[] {
                        // id              timestamp         temperature
                        new VarCharType(), new BigIntType(), new DoubleType()
                };

        ParquetColumnarRowInputFormat<FileSourceSplit> inputFormat = new ParquetColumnarRowInputFormat<>(new Configuration(),
                           //字段类型          // 字段名
                RowType.of(fieldTypes, new String[]{"id", "timestamp", "temperature"}),
                // 每批次读取500行
                500,
                false,
                true
        );
        // FileSource支持递归读取src/main/files下所有目录，而不是递归一层，这个跟Format不同
        FileSource<RowData> source = FileSource.forBulkFileFormat(inputFormat, new Path("src/main/files"))
                .monitorContinuously(Duration.ofSeconds(1))
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 配置水位线策略
        WatermarkStrategy<RowData> watermarkStrategy = WatermarkStrategy.<RowData>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> event.getLong(1));
        // 获取数据源
        DataStreamSource<RowData> sourceStream = env.fromSource(source, watermarkStrategy, "parquet-file-source");
        // 将RowData转为SensorReading
        sourceStream.process(new ProcessFunction<RowData, SensorReading>() {
            @Override
            public void processElement(RowData value, Context ctx, Collector<SensorReading> out) throws Exception {
                out.collect(new SensorReading(value.getString(0).toString(),value.getLong(1),value.getDouble(2)));
            }
        })
                //开窗统计窗口内每个传感器一秒钟的平均读数
                .keyBy(SensorReading::getId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(200)))
                .aggregate(new AvgTempFunction())
                .print();

        env.execute();
    }
}
