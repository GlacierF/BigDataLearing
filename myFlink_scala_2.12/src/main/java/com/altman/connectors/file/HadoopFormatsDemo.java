package com.altman.connectors.file;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.HadoopInputs;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * 使用{@link TextInputFormat}读取hadoop文件，注意路径只能往下读一层
 * @author : Altman
 */
public class HadoopFormatsDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<LongWritable, Text>> input =
                env.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(),
                        LongWritable.class, Text.class, "src/main/files/aaa"));
        input.print();
        env.execute();
    }
}
