package com.altman.connectors.file;

import com.altman.util.SensorReading;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * formats只能batch模式读取文件，一旦读取完毕任务就结束了
 * @author : Altman
 */
public class AvroFormatReaderDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        AvroInputFormat<SensorReading> avroInputFormat = new AvroInputFormat<>(
                //注意 这里只会往下递归一层去找所有的文件，如果配置到src/main/files，那么读取不到任何文件！
                new Path("src/main/files/2021-12-21-07--59/"), SensorReading.class);
        DataStreamSource<SensorReading> input = env.createInput(avroInputFormat);
        input.print();
        env.execute();
    }
}
