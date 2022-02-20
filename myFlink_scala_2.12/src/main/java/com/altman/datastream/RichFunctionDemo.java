package com.altman.datastream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Created on 2021/11/25 21:52
 * Copyright (c) 2021/11/25,一个很有个性的名字 所有
 *
 * @author : Altman
 */
public class RichFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.fromElements("a,b,c", "d,e,f", "g,h,j");
        SingleOutputStreamOperator<String> flatmap = source.flatMap(new RichFlatMapFunction<String, String>() {
            private String prefix;
            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化工作，此方法是当前算子任务最开始执行的方法，且一个算子任务只会执行一次
                prefix =  getRuntimeContext().getIndexOfThisSubtask() + "->";
            }
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String word : value.split(",")) {
                    out.collect(prefix + word);
                }
            }
            @Override
            public void close() throws Exception {
                //资源清理工作  此方法是当前算子任务最后执行的方法，且一个算子任务只会执行一次
            }
        });
        flatmap.print();
        env.execute("RichFunction Test");
    }
}
