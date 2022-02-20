package com.altman.connectors.file;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * 读取text类型的文件，每行都是一条记录
 * @author : Altman
 */
public class TextFileSourceDemo {
    public static void main(String[] args) throws Exception {

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineFormat(), new Path("src/main/files_text"))
                // 持续监控，表示这是一个stream类型任务
                .monitorContinuously(Duration.ofSeconds(1))
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(),"text-file-source")
                .print();

        env.execute();
    }
}
