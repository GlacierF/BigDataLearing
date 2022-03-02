package com.altman.connectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

public class TextFileIncrementSourceDemo {
    public static void main(String[] args) throws Exception{
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);

        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        DataStreamSource<String> dataStream = env.addSource(new SourceFromFile()).setParallelism(1);
        dataStream.print();


        env.execute();
    }

    private static class SourceFromFile extends RichSourceFunction<String> {
        private volatile Boolean isRunning = true;

        @Override
        public void run(SourceContext ctx) throws Exception {
            BufferedReader bufferedReader = new BufferedReader(
                    new FileReader("D:\\个人文档\\我的学习\\BigDataLearing-main" +
                            "\\BigDataLearing-main\\myFlink_scala_2.12\\src\\main\\files\\test"));
            while (isRunning) {
                String line = bufferedReader.readLine();
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                ctx.collect(line);
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
